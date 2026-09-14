"""
slick-stream-buffer-multiplexer-py: A lock-free multi-producer multi-consumer
byte stream multiplexer, binary-compatible with the C++
slick::stream_buffer_multiplexer (slick/stream_buffer_multiplexer.hpp v2.0.0).

Each producer owns its own SlickStreamBuffer (an independently-sized data ring
with its own control ring). consume() additionally publishes a small 16-byte
{sequence, producer_id} record into one shared SlickQueue, which acts as the
lock-free MPMC fan-in / global ordering point.

Consumers call read(cursor), which dequeues the next shared-queue record and
dereferences it back into the matching producer's stream buffer, returning a
(data, length, producer_id) message.

Feature configuration is a `traits` constructor argument - and it is
slick-queue-py's QueueTraits, the traits of the shared record queue this is
built on, not a traits type of its own. Every knob the multiplexer has beyond
that queue is a passthrough, and its one real tunable asks the same question
QueueTraits.enable_loss_detection already asks, so a separate flag would only
let the two disagree. That matters because loss_count() SUMS the two counters:
a half-configured pair would return a partial total that reads like a complete
one. One flag, one number.

One trait is not local to this instance: QueueTraits.enable_read_last is part of
the shared queue's header protocol ('SLQ1' when on, 'SLQ0' when off), and every
peer of a shared-memory queue - Python or C++ - must agree on it or construction
raises "Shared memory feature mismatch". The multiplexer never calls read_last(),
so turning it off is safe, but only if every peer does.

Producers and the shared queue each independently choose local memory or
shared memory (IPC): see add_producer() and the constructor. A cross-process
consumer only needs to register (via add_producer) the producer ids whose
shared memory it has access to; records referencing other producer ids are
silently skipped (not counted as loss - see loss_count()).

Binary compatibility: the multiplexer layer adds no shared-memory structures
of its own. The shared queue is a standard slick-queue segment ('SLQ1', or
'SLQ0' when created with enable_read_last off; element_size=16) whose elements
hold:

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
- loss_count() sums two counters, both switched together by
  traits.enable_loss_detection: the shared queue's own wrap loss and this
  instance's multiplexer-level loss. Only the second is filtered by producer
  registration - see loss_count(). A third, each ProducerBuffer's inner-ring
  loss, is separate, and the multiplexer's own reads never touch it: it only
  moves for reads the caller makes through ProducerBuffer.stream_buffer.read()
  with a traits whose count_loss is true.
- A single message (one consume() call) is limited to < 4 GiB; consume() raises
  ValueError rather than truncating one that is not, propagated from
  SlickStreamBuffer.consume() before any state moves - so nothing is published
  to either the producer's ring or the shared record queue.
"""
from __future__ import annotations

__version__ = '2.0.0'

import operator
import struct
from types import MappingProxyType
from typing import Dict, Mapping, Optional, Tuple, Union

from slick_queue_py import (
    SlickQueue,
    QueueTraits,
    default_queue_traits,
    validate_traits,
)
from slick_stream_buffer_py import SlickStreamBuffer, PublishedRecord, read_traits
from atomic_ops import AtomicCursor  # from slick-queue-py


class _DereferenceReadTraits(read_traits):
    """Read traits for the dereference step (C++ detail::dereference_read_traits).

    _dereference() builds a fresh cursor pointing straight at one record's own
    sequence and then checks the exact sequence it got back, so neither
    read_traits knob earns its cost here:

    - detect_reset rewinds a cursor to 0 on seq >= next_seq_, for a consumer
      whose cursor outlived a reset(). This cursor is one statement old and
      outlives nothing. The rewind can only make read() return record 0, which
      the sequence check then rejects anyway - so it buys a load of next_seq_
      per dereference and no accuracy.
    - count_loss adds (seq - cursor) when a slot holds a newer record. For a
      sequential scan that is the number of records skipped; for a jump-read it
      is "how far the producer has run past this one record", which every later
      lapped dereference would add all over again. The multiplexer counts one
      loss per lapped record itself, which is the number that means something.
    """
    detect_reset = False
    count_loss = False

# Shared-queue element (matches C++ slick::multiplexer_record):
#   uint64 sequence @0 | uint32 producer_id @8 | uint32 pad0 @12
RECORD_FMT = "<QII"
RECORD_SIZE = struct.calcsize(RECORD_FMT)
assert RECORD_SIZE == 16, "record must be 16 bytes"
MAX_PRODUCER_ID = 0xFFFFFFFF  # producer_id is the record's uint32 field


class MultiplexRecord:
    """A dereferenced message as returned by read(). Mirrors C++
    slick::multiplex_record.

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
    shared record queue. Mirrors C++ producer_buffer.

    Created only by StreamBufferMultiplexer.add_producer(). C++ additionally
    deletes producer_buffer's copy and move operations, because the multiplexer
    keeps a raw pointer to the same object its shared_ptr names; Python has no
    moves and rebinding a name copies a reference, so there is nothing to
    delete here.
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

        Raises:
            ValueError: if the message does not fit a record's 32-bit length
                field (a single message must be < 4 GiB), propagated from
                SlickStreamBuffer.consume(). It is raised before any state
                moves, so nothing is published to either the producer's ring or
                the shared record queue and the bytes stay readable - publish
                them as several smaller records instead.
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
        """THIS producer's inner-ring loss only (not multiplexer-level loss).

        The multiplexer dereferences records through traits that do not count
        (see _DereferenceReadTraits), so this only moves for reads the caller
        makes directly through ``stream_buffer.read(cursor, traits)`` with a
        traits whose ``count_loss`` is true. Multiplexer-level loss is the
        multiplexer's own loss_count().
        """
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

    Construction modes (mirroring the three C++ constructors):

    - ``StreamBufferMultiplexer(shared_queue_size=1024)``
      local-memory shared record queue
    - ``StreamBufferMultiplexer(shared_queue_size=1024, name="mux_q")``
      shared-memory record queue (create or open)
    - ``StreamBufferMultiplexer(name="mux_q")``
      open an existing shared-memory record queue (geometry self-described)

    Args:
        shared_queue_size: Shared record queue capacity (must be a power of 2).
        name: Shared memory name for the record queue. If None, local memory.
        traits: Feature configuration, a :class:`QueueTraits` subclass, passed
            straight through to the shared record queue. Defaults to
            ``default_queue_traits``. ``enable_loss_detection`` switches both
            terms of :meth:`loss_count` together. ``enable_read_last`` is
            recorded in a shared queue's header ('SLQ1' on, 'SLQ0' off) and
            must match on every Python and C++ peer, or attaching raises
            RuntimeError ("Shared memory feature mismatch").
    """

    def __init__(self, *, shared_queue_size: Optional[int] = None,
                 name: Optional[str] = None, traits=None):
        if traits is None:
            traits = default_queue_traits
        validate_traits(traits)
        if shared_queue_size is not None and (
                shared_queue_size == 0 or shared_queue_size & (shared_queue_size - 1)):
            raise ValueError("shared_queue_size must be a power of two")
        if shared_queue_size is not None:
            if name is not None:
                self._shared_queue = SlickQueue(
                    name=name, size=shared_queue_size, element_size=RECORD_SIZE,
                    traits=traits)
            else:
                self._shared_queue = SlickQueue(
                    size=shared_queue_size, element_size=RECORD_SIZE, traits=traits)
        else:
            if name is None:
                raise ValueError("shared_queue_size or name must be provided")
            self._shared_queue = SlickQueue(name=name, element_size=RECORD_SIZE,
                                            traits=traits)
        # the queue's SharedMemory, held for unlink(): SlickQueue.close() drops
        # its own handle, after which SlickQueue.unlink() silently does nothing -
        # yet close-then-unlink is the documented order. None for local memory.
        self._shared_queue_shm = getattr(self._shared_queue, '_shm', None)

        self._producers: Dict[int, ProducerBuffer] = {}
        # read-only live view handed out by get_producer_buffers(); built once,
        # so _producers must only ever be mutated in place, never rebound
        self._producers_view: Mapping[int, ProducerBuffer] = MappingProxyType(
            self._producers)
        # multiplexer-level loss (registered producer lapped before dereference);
        # per-instance, like the C++ loss_count_ member. Read the flag off the
        # queue's own frozen snapshot rather than keeping a second copy: the two
        # terms loss_count() sums must never be able to disagree.
        self._count_loss = self._shared_queue.traits.enable_loss_detection
        self._loss_count = 0

    @property
    def traits(self):
        """The frozen traits this multiplexer and its shared queue were built
        with (C++ spells this ``using traits = Traits``)."""
        return self._shared_queue.traits

    @staticmethod
    def remove(name: str) -> bool:
        """Remove a shared memory segment by name, the stale-segment recovery step.

        Applies to both kinds of name this class creates - a shared record
        queue's and a producer stream buffer's - since both are plain shared
        memory segments underneath. Use it when a previous run died
        mid-initialization and left a segment wedged, which construction over
        that name reports by raising::

            StreamBufferMultiplexer.remove("md_records")   # clear a dead run
            mux = StreamBufferMultiplexer(shared_queue_size=1024, name="md_records")

        Returns:
            True if the segment was removed (always True on Windows).

        Warning:
            Only call this when no process is using the segment. On POSIX the
            name is unlinked immediately, so the next creator gets a fresh
            segment while any process still mapped to the old one keeps reading
            the orphaned copy. On Windows this is a no-op returning True - a
            section there disappears once the last handle closes.
        """
        return SlickStreamBuffer.remove(name)

    # ------------------------------------------------------------------
    # Producer registration (single-threaded setup)
    # ------------------------------------------------------------------

    def add_producer(self, producer_id: int, *, capacity: Optional[int] = None,
                     control_size: Optional[int] = None,
                     name: Optional[str] = None) -> ProducerBuffer:
        """Register a producer, mirroring the three C++ overloads:

        - ``add_producer(id, capacity=..., control_size=...)`` local memory
        - ``add_producer(id, capacity=..., control_size=..., name=...)`` shm creator
        - ``add_producer(id, name=...)`` open existing shm (geometry self-described)

        Raises:
            TypeError: if producer_id is not an integer.
            ValueError: if producer_id is outside the record's uint32 field
                (0 <= producer_id <= 0xFFFFFFFF), already registered, or sizes
                invalid. The id is checked here, before any segment is created,
                because consume() packs it only after the inner stream buffer
                has already published - a bad id caught there would leave a
                record in the producer's ring that no multiplexer reader can
                ever find.
        """
        producer_id = operator.index(producer_id)
        if not 0 <= producer_id <= MAX_PRODUCER_ID:
            raise ValueError(
                f"producer_id {producer_id} out of range [0, {MAX_PRODUCER_ID:#x}]")
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

    def get_producer_buffers(self) -> Mapping[int, ProducerBuffer]:
        """Every registered producer, keyed by producer_id.

        A read-only live view of the multiplexer's own registration table, not a
        snapshot: later add_producer() calls show through, and the same cached
        view is returned every call - nothing is allocated, so it is cheap
        enough to call in a monitoring loop. Mutating it raises (C++ returns a
        const map), because unregistering a producer would make read() silently
        skip its queued records; register with add_producer(). Order is the dict's
        insertion order, which C++ (an unordered_map) does not promise, so sort
        by producer_id at the call site if a report needs a fixed sequence::

            total = sum(p.loss_count() for p in mux.get_producer_buffers().values())

        C++ additionally keeps a dense vector fast path for low producer_id
        values; a Python dict lookup is already O(1), so there is nothing to
        mirror.
        """
        return self._producers_view

    def producer_count(self) -> int:
        """Number of registered producers on this multiplexer instance."""
        return len(self._producers)

    # ------------------------------------------------------------------
    # Consumer side
    # ------------------------------------------------------------------

    def read(self, cursor: Union[int, AtomicCursor]) -> Tuple[MultiplexRecord, int]:
        """Read the next message in global order.

        Two modes, mirroring the two C++ overloads:

        - int cursor (per-consumer): ``rec, cursor = mux.read(cursor)`` -
          the updated cursor is returned (Python has no by-ref ints)
        - AtomicCursor (work-stealing across consumers): ``rec, idx = mux.read(cursor)`` -
          the cursor advances atomically inside the shared queue; idx is the
          claimed shared-queue index (-1 when no message was available)

        Returns:
            (record, cursor_or_index) - record is falsy when no message is
            available. Records from unregistered producer ids are silently
            skipped (not loss); records lapped by their producer's ring are
            skipped, and counted in loss_count() when
            ``traits.enable_loss_detection`` is set.
        """
        if isinstance(cursor, AtomicCursor):
            return self._read_atomic(cursor)
        return self._read_int(cursor)

    def loss_count(self) -> int:
        """Shared-queue wrap loss plus multiplexer-level loss (shared-queue
        entries whose producer_id is registered on this instance but were
        lapped by that producer's own ring before being dereferenced).

        Only the second term is filtered by registration. An entry this instance
        actually reads and finds to name an unregistered producer is skipped
        without counting - but the first term counts entries the shared queue
        dropped by wrapping, and a lapped slot has already been overwritten, so
        nothing is left to say which producer it named. An instance that
        deliberately registers a subset of the producers therefore sees wrap
        loss for producers it does not care about, making this an upper bound on
        what it actually missed. Size the shared queue so it does not wrap and
        the term goes to 0, leaving the total exactly this instance's own loss.

        Both terms are switched together by ``traits.enable_loss_detection`` and
        read 0 when it is off. Records are skipped correctly either way; only the
        counters are silent.
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
        """Close all producer buffers and the shared record queue.

        The registration table is kept: unlink() after close() - the documented
        order - still needs every producer's segment name. Closing twice is a
        no-op.
        """
        for producer in self._producers.values():
            try:
                producer.close()
            except Exception:
                pass
        try:
            self._shared_queue.close()
        except Exception:
            pass

    def unlink(self) -> None:
        """Unlink (delete) the shared record queue segment and all registered
        producers' segments.

        Valid before or after close(); the documented order is the
        ``shared_memory`` idiom - close every handle, then unlink once. Deleting
        a name never touches mappings that already exist. Unlinking twice is a
        no-op, so a repeat can never delete a segment a later run has since
        created under the same name.
        """
        for producer in self._producers.values():
            try:
                producer.unlink()  # by saved name, once - valid after close()
            except Exception:
                pass
        shm = self._shared_queue_shm
        if shm is None:
            return
        self._shared_queue_shm = None  # unlink once, however many times this is called
        try:
            shm.unlink()
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
        dereferenced. (Matches C++ dereference().)

        The exact-sequence check is what makes this a dereference rather than a
        scan: read() happily skips forward to whatever the slot now holds, and
        only new_cursor says whether that was the record we asked for.
        """
        data, length, new_cursor = producer.stream_buffer.read(
            sequence, _DereferenceReadTraits)
        if data is not None and new_cursor == sequence + 1:
            return MultiplexRecord(data, length, producer_id)
        return MultiplexRecord()

    def _read_int(self, cursor: int) -> Tuple[MultiplexRecord, int]:
        """read_impl with a per-consumer int cursor."""
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
            if self._count_loss:
                self._loss_count += 1

    def _read_atomic(self, cursor: AtomicCursor) -> Tuple[MultiplexRecord, int]:
        """read_impl with a shared atomic work-stealing cursor."""
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
            if self._count_loss:
                self._loss_count += 1


__all__ = ["StreamBufferMultiplexer", "ProducerBuffer", "MultiplexRecord",
           "AtomicCursor", "QueueTraits", "default_queue_traits",
           "RECORD_SIZE", "__version__"]
