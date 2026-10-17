"""
slick-stream-buffer-multiplexer-py: A lock-free multi-producer multi-consumer
byte stream multiplexer, binary-compatible with the C++
slick::stream_buffer_multiplexer (slick/stream_buffer_multiplexer.hpp v2.2.0).

Each producer owns its own byte ring, of one of two kinds sharing one
producer_id space:

- a SlickStreamBuffer (add_producer(), driven through ProducerBuffer): an
  independently-sized data ring with its own control ring, written with
  prepare/commit/consume from one thread;
- a byte SlickQueue - element_size 1, the C++ slick::queue<uint8_t>
  (add_queue_producer(), driven through ProducerQueue): reserve / write /
  publish, which several threads may do at once.

Publishing through either handle - ProducerBuffer.consume() or
ProducerQueue.publish() - additionally publishes a small 16-byte
{sequence, producer_id} record into one shared SlickQueue, which acts as the
lock-free MPMC fan-in / global ordering point.

Consumers call read(cursor), which dequeues the next shared-queue record and
dereferences it back into the matching producer's ring, returning a
(data, length, producer_id) message. It is the same for both kinds: length is a
byte count.

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
shared memory (IPC): see add_producer(), add_queue_producer() and the
constructor. A cross-process consumer only needs to register the producer ids
whose shared memory it has access to; records referencing other producer ids
are silently skipped (not counted as loss - see loss_count()).

Binary compatibility: the multiplexer layer adds no shared-memory structures
of its own. The shared queue is a standard slick-queue segment ('SLQ1', or
'SLQ0' when created with enable_read_last off; element_size=16) whose elements
hold:

    Offset 0-7  (8 bytes): uint64 sequence     - record seq in the producer's buffer,
                                                 or a queue producer's reservation index
    Offset 8-11 (4 bytes): uint32 producer_id
    Offset 12-15 (4 bytes): uint32 pad0

Each stream buffer producer is a standard slick-stream-buffer segment ('SSB1'),
and each queue producer a standard slick-queue segment with element_size=1.
A C++ slick::stream_buffer_multiplexer and this class interoperate through
the same segments in any combination of producer/consumer roles.

Caveats (same as C++):
- add_producer() and add_queue_producer() are single-threaded setup: call
  them before any producer or consumer threads start.
- Each ProducerBuffer's producer-side methods (prepare/commit/consume/...)
  must be called from a single thread, same as SlickStreamBuffer. A
  ProducerQueue has no such limit.
- loss_count() sums two counters, both switched together by
  traits.enable_loss_detection: the shared queue's own wrap loss and this
  instance's multiplexer-level loss. Only the second is filtered by producer
  registration - see loss_count(). A third, each producer's inner-ring loss,
  is separate: see ProducerBuffer.loss_count() and ProducerQueue.loss_count(),
  which do not behave alike.
- A single stream buffer message (one consume() call) is limited to < 4 GiB;
  consume() raises ValueError rather than truncating one that is not,
  propagated from SlickStreamBuffer.consume() before any state moves - so
  nothing is published to either the producer's ring or the shared record
  queue. A queue message is limited to the queue's size by SlickQueue.reserve().
"""
from __future__ import annotations

__version__ = '2.2.0'

import enum
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


class ProducerKind(enum.Enum):
    """Which ring a registered producer publishes through. Mirrors C++
    slick::producer_kind."""
    STREAM_BUFFER = 'stream_buffer'  # a SlickStreamBuffer, driven through ProducerBuffer
    QUEUE = 'queue'                  # a byte SlickQueue, driven through ProducerQueue


class _Producer:
    """What every registered producer has in common, whichever ring it
    publishes through. Mirrors C++ basic_stream_buffer_multiplexer::producer.

    The two handles derive from it so the multiplexer can keep one lookup table
    for read() - producer ids are unique across both kinds - and so the fan-in
    into the shared record queue is written once. Each handle dereferences its
    own records (_dereference), which is the Python form of C++'s branch on
    kind() into an inlined dereference.
    """

    _kind: ProducerKind  # set by each subclass

    def __init__(self, producer_id: int, shared_queue: SlickQueue):
        self._producer_id = producer_id
        self._shared_queue = shared_queue

    @property
    def producer_id(self) -> int:
        return self._producer_id

    @property
    def kind(self) -> ProducerKind:
        return self._kind

    def _fan_in(self, sequence: int) -> None:
        """Fan one published message into the shared record queue so
        multiplexer consumers can find it."""
        idx = self._shared_queue.reserve()
        struct.pack_into(RECORD_FMT, self._shared_queue[idx], 0,
                         sequence, self._producer_id, 0)
        self._shared_queue.publish(idx)

    def _dereference(self, sequence: int) -> MultiplexRecord:  # pragma: no cover
        raise NotImplementedError


class ProducerBuffer(_Producer):
    """Per-producer handle: wraps one SlickStreamBuffer and publishes into the
    shared record queue. Mirrors C++ producer_buffer.

    Created only by StreamBufferMultiplexer.add_producer(). C++ additionally
    deletes producer_buffer's copy and move operations, because the multiplexer
    keeps a raw pointer to the same object its shared_ptr names; Python has no
    moves and rebinding a name copies a reference, so there is nothing to
    delete here.
    """

    _kind = ProducerKind.STREAM_BUFFER

    def __init__(self, producer_id: int, shared_queue: SlickQueue,
                 buffer: SlickStreamBuffer):
        super().__init__(producer_id, shared_queue)
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
            self._fan_in(rec.sequence)
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

    def _dereference(self, sequence: int) -> MultiplexRecord:
        """Dereference a shared-queue record into this producer's stream
        buffer. Falsy if the record was lapped before being dereferenced.
        (Matches C++ dereference(producer_buffer&, ...).)

        The exact-sequence check is what makes this a dereference rather than a
        scan: read() happily skips forward to whatever the slot now holds, and
        only new_cursor says whether that was the record we asked for.
        """
        data, length, new_cursor = self._buffer.read(sequence, _DereferenceReadTraits)
        if data is not None and new_cursor == sequence + 1:
            return MultiplexRecord(data, length, self._producer_id)
        return MultiplexRecord()


class ProducerQueue(_Producer):
    """Per-producer handle: wraps one byte SlickQueue (element_size 1, the C++
    slick::queue<uint8_t>) and publishes into the shared record queue. Mirrors
    C++ producer_queue.

    The producer side is SlickQueue's own - reserve(), write through
    ``pq[index]``, publish() - and one message is the n bytes of one
    reserve()/publish() pair. Unlike a ProducerBuffer, several threads may drive
    one ProducerQueue at once: SlickQueue's reserve/publish and the fan-in are
    both lock-free MPMC. Records of concurrent publishers reach the shared queue
    in the order their publish() calls got there, which need not be reservation
    order.

    Only what goes through publish() here is multiplexed. Bytes published on the
    inner queue directly - through ``queue``, or by whoever else holds an
    adopted queue - are never fanned in, so multiplexer consumers do not see
    them.

    Created only by StreamBufferMultiplexer.add_queue_producer().
    """

    _kind = ProducerKind.QUEUE

    def __init__(self, producer_id: int, shared_queue: SlickQueue,
                 queue: SlickQueue):
        super().__init__(producer_id, shared_queue)
        self._queue = queue
        # items_per_slot - 1, for _dereference(): read() advances a cursor by the
        # whole reservation, which is the message length rounded up to this.
        self._item_mask = queue.items_per_slot - 1
        # the queue's SharedMemory, held for unlink() for the same reason the
        # multiplexer holds its record queue's: SlickQueue.close() drops its own
        # handle, after which SlickQueue.unlink() silently does nothing. None for
        # local memory.
        self._shm = getattr(queue, '_shm', None)

    # ------------------------------------------------------------------
    # Producer side (any number of threads) - forwards to the inner queue
    # ------------------------------------------------------------------

    def reserve(self, n: int = 1) -> int:
        """Reserve n contiguous bytes for one message.

        Returns the index to write at through ``pq[index]`` and to hand to
        publish(). The reservation takes n rounded up to a multiple of
        items_per_slot. It never blocks: a consumer that has fallen a whole ring
        behind loses the bytes it overwrites.

        Raises:
            ValueError: if n is 0, and RuntimeError if n exceeds size -
                propagated from SlickQueue.reserve().
        """
        return self._queue.reserve(n)

    def __getitem__(self, index: int) -> memoryview:
        """Writable view of the ring from ``index`` to the end of the data
        array - the Python form of C++'s ``uint8_t* operator[]``.

        A reservation never splits across the end of the ring, so the n bytes
        reserve(n) handed out are the first n of this view: write them as
        ``pq[index][:n] = payload``. Writing past n overwrites whatever lies
        beyond the reservation, exactly as writing past n through the C++
        pointer would.
        """
        q = self._queue
        start = q._data_offset + (index & q.mask)
        return q._buf[start:q._data_offset + q.size]

    def publish(self, index: int, n: int = 1) -> None:
        """Publish the n bytes written at index as one message, and fan it into
        the shared record queue so multiplexer consumers can find it.

        Args:
            index: The index reserve() returned.
            n: The message length in bytes - the n given to the matching reserve().
        """
        self._queue.publish(index, n)
        self._fan_in(index)

    # ------------------------------------------------------------------
    # Shared accessors
    # ------------------------------------------------------------------

    @property
    def size(self) -> int:
        """Capacity of the byte ring."""
        return self._queue.size

    @property
    def items_per_slot(self) -> int:
        """The minimum number of bytes one reserve() consumes; see SlickQueue."""
        return self._queue.items_per_slot

    def loss_count(self) -> int:
        """The inner queue's own loss counter - a diagnostic of that queue, not
        a count of multiplexer records.

        SlickQueue configures loss detection per queue, not per call, so unlike
        ProducerBuffer.loss_count() the multiplexer cannot keep its
        dereferences out of it: with the queue's traits counting (slick-queue-py
        defaults to on), each one that finds its record lapped adds SlickQueue's
        "how far the slot has moved on" distance, and every later lapped
        dereference adds its own all over again. Multiplexer-level loss - one per
        lapped record - is the multiplexer's loss_count().
        """
        return self._queue.loss_count()

    def initial_reading_index(self) -> int:
        return self._queue.initial_reading_index()

    @property
    def own_buffer(self) -> bool:
        """True for a local queue and for the shared-memory segment's creator."""
        return not self._queue.use_shm or self._queue._own

    @property
    def use_shm(self) -> bool:
        return self._queue.use_shm

    @property
    def queue(self) -> SlickQueue:
        """The underlying SlickQueue (C++ exposes both queue() and queue_ptr();
        Python needs only one accessor). Publishing through it directly bypasses
        the multiplexer - see the class docstring."""
        return self._queue

    def get_shm_name(self) -> Optional[str]:
        """The queue segment's name for C++ interop (see
        SlickQueue.get_shm_name())."""
        return self._queue.get_shm_name()

    def close(self) -> None:
        """Close the underlying queue connection."""
        self._queue.close()

    def unlink(self) -> None:
        """Unlink (delete) the underlying shared memory segment, if any. Valid
        before or after close(), and a no-op when repeated."""
        shm = self._shm
        if shm is None:
            return
        self._shm = None  # unlink once, however many times this is called
        try:
            shm.unlink()
        except Exception:
            pass

    def _dereference(self, sequence: int) -> MultiplexRecord:
        """Dereference a shared-queue record into this producer's queue. Falsy
        if the record was lapped before being dereferenced - its control slot or
        its bytes. (Matches C++ dereference(producer_queue&, ...).)
        """
        q = self._queue
        data, length, new_cursor = q.read(sequence)
        # Same exact-hit check as the stream buffer's, with SlickQueue's
        # arithmetic: read() leaves the cursor one whole reservation past the
        # index it actually read - the length rounded up to items_per_slot. A
        # lapped slot, or a wrap marker sending the cursor elsewhere, both hand
        # back a message that started at some other index, and the cursor shows it.
        footprint = (length + self._item_mask) & ~self._item_mask
        if data is None or new_cursor != sequence + footprint:
            return MultiplexRecord()
        # An intact control slot does not mean intact bytes. Messages differ in
        # length, so a later reservation can cover this message's bytes without
        # landing on its slot: in an 8-byte ring, "ijkl" at index 8 overwrites
        # positions 0-3 and with them "CD" at 2, whose slot still reads 2.
        # Reservations take indices in order, so the first byte is gone exactly
        # once the reservation counter passes sequence + size.
        #
        # read() has already copied the bytes, so a check made now covers the copy
        # itself: a producer cannot overwrite them after it passes, which in C++
        # (handing back a pointer) it still can. It stays conservative as in C++:
        # a wrapping reservation skips the rest of a lap, so the counter can pass
        # that mark through indices nothing wrote, and a message lying in the
        # skipped tail is reported lost while its bytes are still there.
        #
        # end < sequence: the queue was reset() under this record.
        end = q.initial_reading_index()
        if end < sequence or end - sequence > q.size:
            return MultiplexRecord()
        return MultiplexRecord(data, length, self._producer_id)


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
        # the class itself, for add_queue_producer(): each queue producer's ring is
        # built with the same traits as the record queue, as in C++
        self._traits_class = traits
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

        # One table per kind - what get_producer_buffers() / get_producer_queues()
        # hand out - plus one of every producer for read(), the counterpart of the
        # C++ dense table: producer ids are unique across both kinds, so a record
        # resolves with a single lookup whichever kind published it.
        self._producers: Dict[int, ProducerBuffer] = {}
        self._queue_producers: Dict[int, ProducerQueue] = {}
        self._sources: Dict[int, _Producer] = {}
        # read-only live views handed out by get_producer_buffers() and
        # get_producer_queues(); built once, so the tables must only ever be
        # mutated in place, never rebound
        self._producers_view: Mapping[int, ProducerBuffer] = MappingProxyType(
            self._producers)
        self._queue_producers_view: Mapping[int, ProducerQueue] = MappingProxyType(
            self._queue_producers)
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

        Applies to every kind of name this class creates - a shared record
        queue's, a producer stream buffer's and a producer queue's - since all
        are plain shared memory segments underneath. Use it when a previous run died
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
    #
    # producer_id is one namespace across both kinds: an id held by a stream
    # buffer producer cannot be registered again as a queue producer, or the
    # other way round.
    # ------------------------------------------------------------------

    def add_producer(self, producer_id: int, *, capacity: Optional[int] = None,
                     control_size: Optional[int] = None,
                     name: Optional[str] = None) -> ProducerBuffer:
        """Register a stream buffer producer, mirroring the three C++ overloads:

        - ``add_producer(id, capacity=..., control_size=...)`` local memory
        - ``add_producer(id, capacity=..., control_size=..., name=...)`` shm creator
        - ``add_producer(id, name=...)`` open existing shm (geometry self-described)

        Raises:
            TypeError: if producer_id is not an integer.
            ValueError: if producer_id is outside the record's uint32 field
                (0 <= producer_id <= 0xFFFFFFFF), already registered as either
                kind, or sizes invalid. The id is checked here, before any
                segment is created, because consume() packs it only after the
                inner stream buffer has already published - a bad id caught there
                would leave a record in the producer's ring that no multiplexer
                reader can ever find.
        """
        producer_id = self._check_new_producer_id(producer_id)
        buffer = SlickStreamBuffer(capacity=capacity, control_size=control_size,
                                   name=name)
        return self._register(self._producers,
                              ProducerBuffer(producer_id, self._shared_queue, buffer))

    def add_queue_producer(self, producer_id: int, *, size: Optional[int] = None,
                           items_per_slot: Optional[int] = None,
                           name: Optional[str] = None,
                           queue: Optional[SlickQueue] = None) -> ProducerQueue:
        """Register a queue producer: a byte SlickQueue (element_size 1, the
        C++ slick::queue<uint8_t>) published through a ProducerQueue. Mirrors
        the C++ overloads:

        - ``add_queue_producer(id, size=...)`` local memory, one control slot
          per byte - fine for a small ring; for a large one pass
          ``items_per_slot``, or the 16-byte slots dwarf the data
        - ``add_queue_producer(id, size=..., items_per_slot=...)`` local memory
          whose control slots each cover items_per_slot bytes - the minimum one
          reserve() consumes; see SlickQueue
        - ``add_queue_producer(id, size=..., [items_per_slot=...,] name=...)``
          shm creator
        - ``add_queue_producer(id, name=...)`` open existing shm (size and
          items_per_slot self-described; an explicit items_per_slot must match)
        - ``add_queue_producer(id, queue=q)`` adopt a byte SlickQueue the caller
          already has. Only messages published through the returned
          ProducerQueue are multiplexed; whatever else writes to the queue
          directly stays invisible to read(), while still using up the ring - so
          it can lap messages that were multiplexed.

        The queue is built with this multiplexer's traits, as in C++, where it
        is ``slick::queue<uint8_t, Traits>``. An adopted queue keeps its own; only
        enable_read_last reaches shared memory, and slick-queue-py checks that
        against the segment itself.

        Raises:
            TypeError: if producer_id is not an integer, or queue is not a
                SlickQueue.
            ValueError: if producer_id is outside the record's uint32 field,
                already registered as either kind, the sizes are invalid, queue
                is combined with size, items_per_slot or name, or an adopted
                queue's element_size is not 1.
        """
        producer_id = self._check_new_producer_id(producer_id)
        if queue is not None:
            if size is not None or items_per_slot is not None or name is not None:
                raise ValueError("queue cannot be combined with size, items_per_slot or name")
            if not isinstance(queue, SlickQueue):
                raise TypeError(f"queue must be a SlickQueue, not {type(queue).__name__}")
            if queue.element_size != 1:
                raise ValueError(
                    f"queue element_size must be 1 (a byte queue), not {queue.element_size}")
        elif size is None and name is None:
            raise ValueError("size or name must be provided")
        else:
            queue = SlickQueue(name=name, size=size, element_size=1,
                               traits=self._traits_class, items_per_slot=items_per_slot)
        return self._register(self._queue_producers,
                              ProducerQueue(producer_id, self._shared_queue, queue))

    def has_producer(self, producer_id: int) -> bool:
        """True if producer_id is registered on this instance, as either kind."""
        return producer_id in self._sources

    def get_producer_buffer(self, producer_id: int) -> Optional[ProducerBuffer]:
        """The registered stream buffer producer, or None if producer_id is
        unregistered or is registered as a queue producer."""
        return self._producers.get(producer_id)

    def find_producer(self, producer_id: int) -> Optional[ProducerBuffer]:
        """Alias of get_producer_buffer() (C++ has both; Python has no
        owning/non-owning pointer distinction)."""
        return self._producers.get(producer_id)

    def get_producer_queue(self, producer_id: int) -> Optional[ProducerQueue]:
        """The registered queue producer, or None if producer_id is
        unregistered or is registered as a stream buffer producer."""
        return self._queue_producers.get(producer_id)

    def find_producer_queue(self, producer_id: int) -> Optional[ProducerQueue]:
        """Alias of get_producer_queue() (C++ has both; Python has no
        owning/non-owning pointer distinction)."""
        return self._queue_producers.get(producer_id)

    def get_producer_queues(self) -> Mapping[int, ProducerQueue]:
        """Every registered queue producer, keyed by producer_id.

        The counterpart of get_producer_buffers(), with the same contract: a
        cached read-only live view of the registration table, not a snapshot.
        The two tables never share a key, since producer_id is unique across
        both kinds.
        """
        return self._queue_producers_view

    def get_producer_buffers(self) -> Mapping[int, ProducerBuffer]:
        """Every registered stream buffer producer, keyed by producer_id. Queue
        producers are in get_producer_queues().

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
        """Number of registered producers on this multiplexer instance, of both
        kinds."""
        return len(self._sources)

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
        lapped by that producer's own ring before being dereferenced - for a
        queue producer, also entries whose bytes may already have been
        overwritten). A lapped entry counts once, whichever kind of producer it
        names.

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
        """Close every producer, of both kinds, and the shared record queue.

        The registration tables are kept: unlink() after close() - the
        documented order - still needs every producer's segment name. Closing
        twice is a no-op.
        """
        for producer in self._sources.values():
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
        producers' segments, of both kinds.

        Valid before or after close(); the documented order is the
        ``shared_memory`` idiom - close every handle, then unlink once. Deleting
        a name never touches mappings that already exist. Unlinking twice is a
        no-op, so a repeat can never delete a segment a later run has since
        created under the same name.
        """
        for producer in self._sources.values():
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

    def _check_new_producer_id(self, producer_id: int) -> int:
        """Validate an id for registration, of either kind, before any segment
        is created; returns it as a plain int."""
        producer_id = operator.index(producer_id)
        if not 0 <= producer_id <= MAX_PRODUCER_ID:
            raise ValueError(
                f"producer_id {producer_id} out of range [0, {MAX_PRODUCER_ID:#x}]")
        if self.has_producer(producer_id):
            raise ValueError(f"producer_id {producer_id} already registered")
        return producer_id

    def _register(self, table: dict, producer: _Producer):
        """Record a constructed handle in its kind's table and in the table
        read() uses - that one last, so read() never sees a producer the typed
        table does not own."""
        table[producer.producer_id] = producer
        self._sources[producer.producer_id] = producer
        return producer

    def _read_int(self, cursor: int) -> Tuple[MultiplexRecord, int]:
        """read_impl with a per-consumer int cursor."""
        while True:
            data, _, cursor = self._shared_queue.read(cursor)
            if data is None:
                return MultiplexRecord(), cursor
            sequence, producer_id, _ = struct.unpack(RECORD_FMT, data)
            producer = self._sources.get(producer_id)
            if producer is None:
                continue  # not registered on this instance: ignored, not loss
            result = producer._dereference(sequence)
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
            producer = self._sources.get(producer_id)
            if producer is None:
                continue
            result = producer._dereference(sequence)
            if result:
                return result, idx
            if self._count_loss:
                self._loss_count += 1


__all__ = ["StreamBufferMultiplexer", "ProducerBuffer", "ProducerQueue", "ProducerKind",
           "MultiplexRecord", "AtomicCursor", "QueueTraits", "default_queue_traits",
           "RECORD_SIZE", "__version__"]
