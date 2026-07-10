"""
slick-stream-buffer-multiplexer-py: A lock-free multi-producer multi-consumer
byte stream multiplexer, binary-compatible with the C++
slick::stream_buffer_multiplexer (slick/stream_buffer_multiplexer.hpp v1.0.x).

Each producer owns its own SlickStreamBuffer (an independently-sized data ring
with its own control ring). consume() additionally publishes a small 16-byte
{sequence, producer_id} record into one shared SlickQueue, which acts as the
lock-free MPMC fan-in / global ordering point.

Consumers call read(cursor), which dequeues the next shared-queue record and
dereferences it back into the matching producer's stream buffer, returning a
(data, length, producer_id) message.

Producers and the shared queue each independently choose local memory or
shared memory (IPC): see add_producer() and the constructor. A cross-process
consumer only needs to register (via add_producer) the producer ids whose
shared memory it has access to; records referencing other producer ids are
silently skipped (not counted as loss - see loss_count()).

Binary compatibility: the multiplexer layer adds no shared-memory structures
of its own. The shared queue is a standard slick-queue segment ('SLQ1',
element_size=16) whose elements hold:

    Offset 0-7  (8 bytes): uint64 sequence     - record seq in the producer's buffer
    Offset 8-11 (4 bytes): uint32 producer_id
    Offset 12-15 (4 bytes): uint32 pad0

Each producer buffer is a standard slick-stream-buffer segment ('SSB1').
A C++ slick::stream_buffer_multiplexer and this class interoperate through
the same segments in any combination of producer/consumer roles.

Caveats (same as C++):
- add_producer() is single-threaded setup: call it before any producer or
  consumer threads start.
- Each ProducerBuffer's producer-side methods (prepare/commit/consume/...)
  must be called from a single thread, same as SlickStreamBuffer.
- Three independent loss counters exist: the shared queue's own
  queue-wrap loss, this multiplexer's loss_count() (which adds
  multiplexer-level loss: shared-queue entries whose producer_id is registered
  on this instance but were lapped by that producer's own ring before being
  dereferenced), and each ProducerBuffer's own loss_count() (inner ring loss).
  Entries whose producer_id is unregistered on this instance are never counted
  as loss.
- A single message (one consume() call) is limited to < 4 GiB.
"""
from __future__ import annotations

__version__ = '0.1.0'

import struct
from typing import Dict, Optional, Tuple, Union

from slick_queue_py import SlickQueue
from slick_stream_buffer_py import SlickStreamBuffer, PublishedRecord
from atomic_ops import AtomicCursor  # from slick-queue-py

# Shared-queue element (matches stream_buffer_multiplexer.hpp:80-85):
#   uint64 sequence @0 | uint32 producer_id @8 | uint32 pad0 @12
RECORD_FMT = "<QII"
RECORD_SIZE = struct.calcsize(RECORD_FMT)
assert RECORD_SIZE == 16, "record must be 16 bytes"


class MultiplexRecord:
    """A dereferenced message as returned by read(). Mirrors C++
    multiplex_record (stream_buffer_multiplexer.hpp:88-93).

    Evaluates to False if no message was available. Unlike C++, `data` is a
    bytes copy, not a pointer into the producer's ring.
    """
    __slots__ = ('data', 'length', 'producer_id')

    def __init__(self, data: Optional[bytes] = None, length: int = 0,
                 producer_id: int = 0):
        self.data = data
        self.length = length
        self.producer_id = producer_id

    def __bool__(self) -> bool:
        return self.data is not None

    def __repr__(self) -> str:
        return (f"MultiplexRecord(producer_id={self.producer_id}, "
                f"length={self.length})")


class ProducerBuffer:
    """Per-producer handle: wraps one SlickStreamBuffer and publishes into the
    shared record queue. Mirrors C++ producer_buffer
    (stream_buffer_multiplexer.hpp:96-168).

    Created only by StreamBufferMultiplexer.add_producer().
    """

    def __init__(self, producer_id: int, shared_queue: SlickQueue,
                 buffer: SlickStreamBuffer):
        self._producer_id = producer_id
        self._shared_queue = shared_queue
        self._buffer = buffer

    # ------------------------------------------------------------------
    # Producer side (single thread only) - forwards to the inner stream buffer
    # ------------------------------------------------------------------

    def prepare(self, n: int) -> memoryview:
        """Get a contiguous writable region of n bytes (zero-copy memoryview)."""
        return self._buffer.prepare(n)

    def commit(self, n: int) -> None:
        """Move n prepared bytes into the readable region (clamped)."""
        self._buffer.commit(n)

    def consume(self, n: int) -> PublishedRecord:
        """Publish the first n committed bytes as one message, and fan it into
        the shared record queue so multiplexer consumers can find it.
        (Matches C++ stream_buffer_multiplexer.hpp:116-127.)
        """
        rec = self._buffer.consume(n)
        if rec:
            idx = self._shared_queue.reserve()
            struct.pack_into(RECORD_FMT, self._shared_queue[idx], 0,
                             rec.sequence, self._producer_id, 0)
            self._shared_queue.publish(idx)
        return rec

    def discard(self) -> None:
        """Drop unconsumed + prepared bytes without publishing."""
        self._buffer.discard()

    def data(self) -> memoryview:
        """Read-only view of the committed-but-unconsumed region."""
        return self._buffer.data()

    def size(self) -> int:
        """Number of committed-but-unconsumed bytes."""
        return self._buffer.size()

    # ------------------------------------------------------------------
    # Shared accessors
    # ------------------------------------------------------------------

    @property
    def capacity(self) -> int:
        return self._buffer.capacity

    @property
    def control_size(self) -> int:
        return self._buffer.control_size

    def loss_count(self) -> int:
        """THIS producer's inner ring loss only (not multiplexer-level loss)."""
        return self._buffer.loss_count()

    def initial_reading_index(self) -> int:
        return self._buffer.initial_reading_index()

    @property
    def own_buffer(self) -> bool:
        return self._buffer.own_buffer

    @property
    def use_shm(self) -> bool:
        return self._buffer.use_shm

    @property
    def producer_id(self) -> int:
        return self._producer_id

    @property
    def stream_buffer(self) -> SlickStreamBuffer:
        """The underlying SlickStreamBuffer (C++ exposes both stream_buffer()
        and stream_buffer_ptr(); Python needs only one accessor)."""
        return self._buffer

    def get_shm_name(self) -> Optional[str]:
        """The producer segment's name for C++ interop (see
        SlickStreamBuffer.get_shm_name())."""
        return self._buffer.get_shm_name()

    def close(self) -> None:
        """Close the underlying stream buffer connection."""
        self._buffer.close()

    def unlink(self) -> None:
        """Unlink (delete) the underlying shared memory segment, if any."""
        self._buffer.unlink()


class StreamBufferMultiplexer:
    """A lock-free MPMC byte stream multiplexer compatible with C++
    slick::stream_buffer_multiplexer.

    Construction modes (mirroring the three C++ constructors,
    stream_buffer_multiplexer.hpp:175-184):

    - ``StreamBufferMultiplexer(shared_queue_size=1024)``
      local-memory shared record queue
    - ``StreamBufferMultiplexer(shared_queue_size=1024, name="mux_q")``
      shared-memory record queue (create or open)
    - ``StreamBufferMultiplexer(name="mux_q")``
      open an existing shared-memory record queue (geometry self-described)

    Args:
        shared_queue_size: Shared record queue capacity (must be a power of 2).
        name: Shared memory name for the record queue. If None, local memory.
    """

    def __init__(self, *, shared_queue_size: Optional[int] = None,
                 name: Optional[str] = None):
        if shared_queue_size is not None and (
                shared_queue_size == 0 or shared_queue_size & (shared_queue_size - 1)):
            raise ValueError("shared_queue_size must be a power of two")
        if shared_queue_size is not None:
            if name is not None:
                self._shared_queue = SlickQueue(
                    name=name, size=shared_queue_size, element_size=RECORD_SIZE)
            else:
                self._shared_queue = SlickQueue(
                    size=shared_queue_size, element_size=RECORD_SIZE)
        else:
            if name is None:
                raise ValueError("shared_queue_size or name must be provided")
            self._shared_queue = SlickQueue(name=name, element_size=RECORD_SIZE)

        self._producers: Dict[int, ProducerBuffer] = {}
        # multiplexer-level loss (registered producer lapped before dereference);
        # per-instance, like the C++ loss_count_ member. Always counted in Python.
        self._loss_count = 0

    # ------------------------------------------------------------------
    # Producer registration (single-threaded setup)
    # ------------------------------------------------------------------

    def add_producer(self, producer_id: int, *, capacity: Optional[int] = None,
                     control_size: Optional[int] = None,
                     name: Optional[str] = None) -> ProducerBuffer:
        """Register a producer, mirroring the three C++ overloads
        (stream_buffer_multiplexer.hpp:194-206):

        - ``add_producer(id, capacity=..., control_size=...)`` local memory
        - ``add_producer(id, capacity=..., control_size=..., name=...)`` shm creator
        - ``add_producer(id, name=...)`` open existing shm (geometry self-described)

        Raises:
            ValueError: if producer_id is already registered, or sizes invalid.
        """
        if self.has_producer(producer_id):
            raise ValueError(f"producer_id {producer_id} already registered")
        buffer = SlickStreamBuffer(capacity=capacity, control_size=control_size,
                                   name=name)
        producer = ProducerBuffer(producer_id, self._shared_queue, buffer)
        self._producers[producer_id] = producer
        return producer

    def has_producer(self, producer_id: int) -> bool:
        return producer_id in self._producers

    def get_producer_buffer(self, producer_id: int) -> Optional[ProducerBuffer]:
        """The registered producer, or None if producer_id is unregistered."""
        return self._producers.get(producer_id)

    def find_producer(self, producer_id: int) -> Optional[ProducerBuffer]:
        """Alias of get_producer_buffer() (C++ has both; Python has no
        owning/non-owning pointer distinction)."""
        return self._producers.get(producer_id)

    def producer_count(self) -> int:
        """Number of registered producers on this multiplexer instance."""
        return len(self._producers)

    # ------------------------------------------------------------------
    # Consumer side
    # ------------------------------------------------------------------

    def read(self, cursor: Union[int, AtomicCursor]) -> Tuple[MultiplexRecord, int]:
        """Read the next message in global order.

        Two modes, mirroring the two C++ overloads
        (stream_buffer_multiplexer.hpp:237-248):

        - int cursor (per-consumer): ``rec, cursor = mux.read(cursor)`` -
          the updated cursor is returned (Python has no by-ref ints)
        - AtomicCursor (work-stealing across consumers): ``rec, idx = mux.read(cursor)`` -
          the cursor advances atomically inside the shared queue; idx is the
          claimed shared-queue index (-1 when no message was available)

        Returns:
            (record, cursor_or_index) - record is falsy when no message is
            available. Records from unregistered producer ids are silently
            skipped (not loss); records lapped by their producer's ring are
            skipped and counted in loss_count().
        """
        if isinstance(cursor, AtomicCursor):
            return self._read_atomic(cursor)
        return self._read_int(cursor)

    def loss_count(self) -> int:
        """Shared-queue wrap loss plus multiplexer-level loss (shared-queue
        entries whose producer_id is registered on this instance but were
        lapped by that producer's own ring before being dereferenced).
        Entries whose producer_id is unregistered are never counted.
        (Matches C++ stream_buffer_multiplexer.hpp:255-261; Python always
        counts, C++ only when the loss-detection macros are enabled.)
        """
        return self._shared_queue.loss_count() + self._loss_count

    def initial_reading_index(self) -> int:
        """Initial reading cursor for a late-joining consumer
        (see SlickQueue.initial_reading_index())."""
        return self._shared_queue.initial_reading_index()

    # ------------------------------------------------------------------
    # Lifecycle
    # ------------------------------------------------------------------

    @property
    def shared_queue(self) -> SlickQueue:
        """The underlying shared record queue."""
        return self._shared_queue

    def get_shm_name(self) -> Optional[str]:
        """The shared record queue's segment name for C++ interop."""
        return self._shared_queue.get_shm_name()

    def close(self) -> None:
        """Close all producer buffers and the shared record queue."""
        for producer in self._producers.values():
            try:
                producer.close()
            except Exception:
                pass
        self._producers = {}
        try:
            self._shared_queue.close()
        except Exception:
            pass

    def unlink(self) -> None:
        """Unlink (delete) the shared record queue segment and all registered
        producers' segments. Call once, after all users have closed."""
        for producer in self._producers.values():
            try:
                producer.unlink()
            except Exception:
                pass
        try:
            self._shared_queue.unlink()
        except Exception:
            pass

    def __enter__(self):
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):  # noqa: U100
        self.close()
        return False

    # ------------------------------------------------------------------
    # Internal
    # ------------------------------------------------------------------

    def _dereference(self, producer: ProducerBuffer, sequence: int,
                     producer_id: int) -> MultiplexRecord:
        """Dereference a shared-queue record into the matching producer's
        stream buffer. Falsy if the record was lapped before being
        dereferenced. (Matches C++ stream_buffer_multiplexer.hpp:315-322.)"""
        data, length, new_cursor = producer.stream_buffer.read(sequence)
        if data is not None and new_cursor == sequence + 1:
            return MultiplexRecord(data, length, producer_id)
        return MultiplexRecord()

    def _read_int(self, cursor: int) -> Tuple[MultiplexRecord, int]:
        """read_impl with a per-consumer int cursor
        (stream_buffer_multiplexer.hpp:324-345)."""
        while True:
            data, _, cursor = self._shared_queue.read(cursor)
            if data is None:
                return MultiplexRecord(), cursor
            sequence, producer_id, _ = struct.unpack(RECORD_FMT, data)
            producer = self._producers.get(producer_id)
            if producer is None:
                continue  # not registered on this instance: ignored, not loss
            result = self._dereference(producer, sequence, producer_id)
            if result:
                return result, cursor
            self._loss_count += 1

    def _read_atomic(self, cursor: AtomicCursor) -> Tuple[MultiplexRecord, int]:
        """read_impl with a shared atomic work-stealing cursor
        (stream_buffer_multiplexer.hpp:246-248, 324-345)."""
        while True:
            data, _, idx = self._shared_queue.read(cursor)
            if data is None:
                return MultiplexRecord(), -1
            sequence, producer_id, _ = struct.unpack(RECORD_FMT, data)
            producer = self._producers.get(producer_id)
            if producer is None:
                continue
            result = self._dereference(producer, sequence, producer_id)
            if result:
                return result, idx
            self._loss_count += 1


__all__ = ["StreamBufferMultiplexer", "ProducerBuffer", "MultiplexRecord",
           "AtomicCursor", "RECORD_SIZE", "__version__"]
