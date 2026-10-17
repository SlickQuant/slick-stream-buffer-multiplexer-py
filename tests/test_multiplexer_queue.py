"""
Queue producer tests for StreamBufferMultiplexer (local memory mode).

Mirrors slick-stream-buffer-multiplexer/tests/multiplexer_queue_tests.cpp
(adapted to the Python API: read() returns (record, cursor), the producer side
writes through ``pq[index][:n]``, data payloads are bytes copies). The C++-only
cases - copy/move deletion, static types, std::bad_alloc during registration -
have no Python counterpart.
"""
import sys
from pathlib import Path

_root = Path(__file__).parent.parent
sys.path.insert(0, str(_root))
sys.path.insert(0, str(_root.parent / "slick-queue-py"))
sys.path.insert(0, str(_root.parent / "slick-stream-buffer-py"))

from slick_stream_buffer_multiplexer_py import (
    StreamBufferMultiplexer,
    ProducerBuffer,
    ProducerQueue,
    ProducerKind,
    AtomicCursor,
    QueueTraits,
)
from slick_queue_py import SlickQueue


class CountingTraits(QueueTraits):
    enable_loss_detection = True


class SilentTraits(QueueTraits):
    enable_loss_detection = False


def publish_message(producer, data: bytes) -> None:
    """Publish one message through either kind of producer handle: reserve +
    write + publish for a ProducerQueue, prepare + write + commit + consume for
    a ProducerBuffer."""
    n = len(data)
    if isinstance(producer, ProducerQueue):
        index = producer.reserve(n)
        producer[index][:n] = data
        producer.publish(index, n)
    else:
        mv = producer.prepare(n)
        mv[:] = data
        producer.commit(n)
        producer.consume(n)


def publish_bytes(producer, count: int) -> None:
    """Publish `count` single-byte messages numbered 0, 1, 2, ..."""
    for i in range(count):
        publish_message(producer, bytes([i]))


def drain(mux, cursor: int = 0):
    """Read until empty; returns the delivered payloads."""
    delivered = []
    while True:
        rec, cursor = mux.read(cursor)
        if not rec:
            return delivered
        delivered.append(rec.data)


def expect_raises(exc_type, func, *args, **kwargs):
    try:
        func(*args, **kwargs)
    except exc_type:
        return
    raise AssertionError(f"expected {exc_type.__name__}")


# ---------------------------------------------------------------------------
# A byte SlickQueue as a producer
# ---------------------------------------------------------------------------

def test_single_queue_producer_roundtrip():
    mux = StreamBufferMultiplexer(shared_queue_size=64)
    q0 = mux.add_queue_producer(0, size=1024)
    assert isinstance(q0, ProducerQueue)
    assert q0.producer_id == 0
    assert q0.kind is ProducerKind.QUEUE

    publish_message(q0, b"hello")

    rec, cursor = mux.read(0)
    assert rec
    assert rec.producer_id == 0
    assert rec.length == 5
    assert rec.data == b"hello"
    assert cursor == 1

    rec, cursor = mux.read(cursor)
    assert not rec
    assert mux.loss_count() == 0
    mux.close()


def test_mixed_producers_keep_publish_order():
    """Both kinds fan into one shared record queue, so a consumer gets one
    stream in publish order however the producers are mixed."""
    mux = StreamBufferMultiplexer(shared_queue_size=64, traits=CountingTraits)
    b0 = mux.add_producer(0, capacity=1024, control_size=16)
    q1 = mux.add_queue_producer(1, size=1024)
    b2 = mux.add_producer(2, capacity=1024, control_size=16)

    assert b0.kind is ProducerKind.STREAM_BUFFER
    assert q1.kind is ProducerKind.QUEUE

    publish_message(b0, b"aaa")
    publish_message(q1, b"bbbb")
    publish_message(b2, b"cc")
    publish_message(q1, b"ddddd")
    publish_message(b0, b"e")

    order = [(0, b"aaa"), (1, b"bbbb"), (2, b"cc"), (1, b"ddddd"), (0, b"e")]
    cursor = 0
    for producer_id, data in order:
        rec, cursor = mux.read(cursor)
        assert rec
        assert rec.producer_id == producer_id
        assert rec.data == data
        assert rec.length == len(data)
    rec, cursor = mux.read(cursor)
    assert not rec
    assert mux.loss_count() == 0
    mux.close()


def test_shared_atomic_cursor_reads_queue_producers():
    """The work-stealing overload goes through the same dereference."""
    mux = StreamBufferMultiplexer(shared_queue_size=64)
    q0 = mux.add_queue_producer(0, size=64)
    publish_bytes(q0, 3)

    cursor_buf = bytearray(8)
    shared_cursor = AtomicCursor(cursor_buf, 0)
    shared_cursor.store(0)
    for want in range(3):
        rec, _ = mux.read(shared_cursor)
        assert rec
        assert rec.length == 1
        assert rec.data[0] == want
    rec, idx = mux.read(shared_cursor)
    assert not rec
    assert idx == -1
    shared_cursor.release()
    mux.close()


def test_items_per_slot_messages_dereference_exactly():
    """With items_per_slot > 1 the queue advances a cursor by the reservation,
    not the message length; the dereference must round the same way, so lengths
    below, at and above the granularity all come back."""
    mux = StreamBufferMultiplexer(shared_queue_size=64, traits=CountingTraits)
    q0 = mux.add_queue_producer(0, size=1024, items_per_slot=64)
    assert q0.size == 1024
    assert q0.items_per_slot == 64

    pattern = bytes((i + 1) & 0xFF for i in range(200))
    lengths = [3, 200, 64, 65, 1]  # 64 + 256 + 64 + 128 + 64 = 576 bytes
    for length in lengths:
        publish_message(q0, pattern[:length])

    assert drain(mux) == [pattern[:length] for length in lengths]
    assert mux.loss_count() == 0
    mux.close()


def test_reservation_that_wraps_the_ring_stays_contiguous():
    """A reservation that does not fit before the end of the ring restarts at
    the beginning rather than splitting; the record names the index the message
    really starts at, so it comes back in one piece."""
    mux = StreamBufferMultiplexer(shared_queue_size=64, traits=CountingTraits)
    q0 = mux.add_queue_producer(0, size=256, items_per_slot=64)

    first = b"\xaa" * 100
    second = bytes(i & 0xFF for i in range(200))

    publish_message(q0, first)  # bytes 0..127 of the ring
    rec, cursor = mux.read(0)
    assert rec and rec.data == first

    # 256 bytes of reservation do not fit in the 128 left, so this one starts over at 256.
    index = q0.reserve(len(second))
    assert index == 256
    q0[index][:len(second)] = second
    q0.publish(index, len(second))

    rec, cursor = mux.read(cursor)
    assert rec
    assert rec.length == 200
    assert rec.data == second
    rec, cursor = mux.read(cursor)
    assert not rec
    assert mux.loss_count() == 0
    mux.close()


def test_getitem_view_spans_the_reservation():
    """pq[index] is the Python form of the C++ pointer: a writable view from the
    index to the end of the data array, of which the reserved n bytes are the
    first n."""
    mux = StreamBufferMultiplexer(shared_queue_size=64)
    q0 = mux.add_queue_producer(0, size=16)
    index = q0.reserve(5)
    view = q0[index]
    assert len(view) == 16
    view[:5] = b"hello"
    q0.publish(index, 5)

    index = q0.reserve(3)
    assert index == 5
    assert len(q0[index]) == 11  # to the end of the ring, past this reservation
    q0[index][:3] = b"abc"
    q0.publish(index, 3)

    assert drain(mux) == [b"hello", b"abc"]
    mux.close()


# ---------------------------------------------------------------------------
# Loss
# ---------------------------------------------------------------------------

def test_queue_lap_causes_multiplexer_loss():
    """A record whose bytes the producer's own ring has since overwritten is
    skipped and counted once."""
    mux = StreamBufferMultiplexer(shared_queue_size=64, traits=CountingTraits)
    q0 = mux.add_queue_producer(0, size=4)  # tiny ring: laps after 4 one-byte messages

    publish_bytes(q0, 10)

    # only the last 4 published messages (6..9) are still present in the ring
    assert drain(mux) == [bytes([6]), bytes([7]), bytes([8]), bytes([9])]
    assert mux.loss_count() == 6
    mux.close()


def test_queue_lap_is_skipped_with_counters_off():
    """Records are skipped correctly with the counters off; only the numbers
    are silent."""
    mux = StreamBufferMultiplexer(shared_queue_size=64, traits=SilentTraits)
    q0 = mux.add_queue_producer(0, size=4)

    publish_bytes(q0, 10)

    assert drain(mux) == [bytes([6]), bytes([7]), bytes([8]), bytes([9])]
    assert mux.loss_count() == 0
    assert q0.loss_count() == 0
    mux.close()


def test_inner_queue_loss_count_is_not_a_record_count():
    """SlickQueue configures loss detection per queue, not per call, so the
    multiplexer's dereferences reach the inner queue's own counter: each lapped
    one adds how far that slot has moved on. Over the ten messages above the six
    lapped dereferences ask for 0..5 and land on slots holding 8, 9, 6, 7, 8, 9:
    8 + 8 + 4 + 4 + 4 + 4 = 32. The multiplexer's own count is the number to read."""
    mux = StreamBufferMultiplexer(shared_queue_size=64, traits=CountingTraits)
    q0 = mux.add_queue_producer(0, size=4)

    publish_bytes(q0, 10)
    assert q0.loss_count() == 0  # nothing has read the queue yet

    drain(mux)
    assert mux.loss_count() == 6
    assert q0.loss_count() == 32
    mux.close()


def test_overwritten_bytes_are_not_returned():
    """Regression (C++ OverwrittenBytesAreNotReturned): messages of different
    lengths lap the ring at different rates than their control slots. In an
    8-byte ring "ijkl" at index 8 covers positions 0-3, so it overwrites "CD" at
    2 as well as "AB" at 0 - but it only lands on slot 0, and "CD"'s slot still
    says index 2. Both overwritten messages must be skipped and counted."""
    mux = StreamBufferMultiplexer(shared_queue_size=64, traits=CountingTraits)
    q0 = mux.add_queue_producer(0, size=8)

    for message in (b"AB", b"CD", b"EFGH", b"ijkl"):
        publish_message(q0, message)

    assert drain(mux) == [b"EFGH", b"ijkl"]
    assert mux.loss_count() == 2
    mux.close()


def test_message_in_a_skipped_tail_is_reported_lost():
    """The documented price of that check: it is conservative. "nopq" does not
    fit at position 5 and restarts at index 16, leaving 13-15 unwritten - and
    "GH" at positions 6-7 lies in that tail, untouched. The reservation counter
    says it is more than a ring old all the same, so it is skipped and counted:
    a false loss, never a corrupt message."""
    mux = StreamBufferMultiplexer(shared_queue_size=64, traits=CountingTraits)
    q0 = mux.add_queue_producer(0, size=8)

    for message in (b"AB", b"CDEF", b"GH", b"ijklm", b"nopq"):
        publish_message(q0, message)

    # "AB" and "ijklm" lost their slots to "nopq", "CDEF" its bytes to "ijklm",
    # and "GH" nothing.
    assert drain(mux) == [b"nopq"]
    assert mux.loss_count() == 4

    # "GH" really is still there: the queue itself hands it back from its own index.
    data, length, _ = q0.queue.read(6)
    assert data == b"GH" and length == 2
    mux.close()


# ---------------------------------------------------------------------------
# Registration
# ---------------------------------------------------------------------------

def test_duplicate_producer_id_across_kinds_raises():
    """producer_id is one namespace across both kinds."""
    mux = StreamBufferMultiplexer(shared_queue_size=64)

    mux.add_producer(0, capacity=1024, control_size=16)
    expect_raises(ValueError, mux.add_queue_producer, 0, size=64)

    mux.add_queue_producer(1, size=64)
    expect_raises(ValueError, mux.add_producer, 1, capacity=1024, control_size=16)
    expect_raises(ValueError, mux.add_queue_producer, 1, size=128)

    assert mux.producer_count() == 2
    assert len(mux.get_producer_buffers()) == 1
    assert len(mux.get_producer_queues()) == 1
    mux.close()


def test_invalid_queue_arguments_raise():
    mux = StreamBufferMultiplexer(shared_queue_size=64)

    expect_raises(ValueError, mux.add_queue_producer, 0, size=1000)                    # not pow2
    expect_raises(ValueError, mux.add_queue_producer, 0, size=0)                       # zero
    expect_raises(ValueError, mux.add_queue_producer, 0, size=1024, items_per_slot=48)  # ips not pow2
    expect_raises(ValueError, mux.add_queue_producer, 0, size=64, items_per_slot=128)   # ips > size
    expect_raises(ValueError, mux.add_queue_producer, 0)                               # nothing given
    expect_raises(ValueError, mux.add_queue_producer, 0x1_0000_0000, size=64)          # id range
    expect_raises(TypeError, mux.add_queue_producer, "0", size=64)                     # id type

    adopted = SlickQueue(size=64, element_size=1)
    expect_raises(ValueError, mux.add_queue_producer, 0, queue=adopted, size=64)       # both
    expect_raises(ValueError, mux.add_queue_producer, 0,
                  queue=SlickQueue(size=64, element_size=8))                           # not bytes
    expect_raises(TypeError, mux.add_queue_producer, 0, queue=bytearray(64))           # not a queue

    # A failed registration leaves nothing behind.
    assert not mux.has_producer(0)
    assert mux.producer_count() == 0

    # reserve() reports a bad length the way SlickQueue does.
    q0 = mux.add_queue_producer(0, size=64)
    expect_raises(ValueError, q0.reserve, 0)
    expect_raises(RuntimeError, q0.reserve, 65)
    mux.close()


def test_accessors_tell_the_two_kinds_apart():
    """Each typed table carries exactly its own kind, and each typed accessor
    refuses an id of the other kind."""
    mux = StreamBufferMultiplexer(shared_queue_size=64)
    mux.add_producer(0, capacity=1024, control_size=16)
    mux.add_queue_producer(1, size=64)
    mux.add_producer(2, capacity=1024, control_size=16)
    mux.add_queue_producer(3, size=64)
    mux.add_producer(9000, capacity=1024, control_size=16)
    mux.add_queue_producer(9001, size=64)

    assert mux.producer_count() == 6
    buffers = mux.get_producer_buffers()
    queues = mux.get_producer_queues()
    assert sorted(buffers) == [0, 2, 9000]
    assert sorted(queues) == [1, 3, 9001]

    for pid, producer in buffers.items():
        assert isinstance(producer, ProducerBuffer)
        assert mux.has_producer(pid)
        assert producer.producer_id == pid
        assert producer.kind is ProducerKind.STREAM_BUFFER
        assert mux.get_producer_buffer(pid) is producer
        assert mux.find_producer(pid) is producer
        assert mux.get_producer_queue(pid) is None
        assert mux.find_producer_queue(pid) is None
    for pid, producer in queues.items():
        assert isinstance(producer, ProducerQueue)
        assert mux.has_producer(pid)
        assert producer.producer_id == pid
        assert producer.kind is ProducerKind.QUEUE
        assert mux.get_producer_queue(pid) is producer
        assert mux.find_producer_queue(pid) is producer
        assert mux.get_producer_buffer(pid) is None
        assert mux.find_producer(pid) is None

    for pid in (4, 100, 9002):
        assert not mux.has_producer(pid)
        assert mux.get_producer_buffer(pid) is None
        assert mux.get_producer_queue(pid) is None
    mux.close()


def test_get_producer_queues_is_a_live_read_only_view():
    """Same contract as get_producer_buffers(): the table itself, not a
    snapshot."""
    mux = StreamBufferMultiplexer(shared_queue_size=64)
    mux.add_producer(0, capacity=1024, control_size=16)
    assert len(mux.get_producer_queues()) == 0  # a stream buffer producer is not in it

    mux.add_queue_producer(1, size=64)
    assert mux.get_producer_queues() is mux.get_producer_queues()
    assert len(mux.get_producer_queues()) == 1
    mux.add_queue_producer(2, size=64)
    assert len(mux.get_producer_queues()) == 2
    assert len(mux.get_producer_buffers()) == 1

    try:
        mux.get_producer_queues()[5] = None
        raise AssertionError("get_producer_queues() must be read-only")
    except TypeError:
        pass
    mux.close()


# ---------------------------------------------------------------------------
# The ProducerQueue handle
# ---------------------------------------------------------------------------

def test_producer_queue_forwarded_api():
    mux = StreamBufferMultiplexer(shared_queue_size=64)
    q0 = mux.add_queue_producer(0, size=256, items_per_slot=16)

    assert q0.size == 256
    assert q0.items_per_slot == 16
    assert q0.loss_count() == 0
    assert q0.initial_reading_index() == 0
    assert q0.own_buffer
    assert not q0.use_shm
    assert q0.get_shm_name() is None
    assert isinstance(q0.queue, SlickQueue)
    assert q0.queue.element_size == 1

    index = q0.reserve(5)
    assert index == 0
    q0[index][:5] = b"hello"
    q0.publish(index, 5)
    assert bytes(q0[index][:5]) == b"hello"
    assert q0.initial_reading_index() == 16  # one whole reservation further on
    mux.close()


def test_queue_traits_follow_the_multiplexer():
    """The queue is built with the multiplexer's traits, as in C++, where it is
    slick::queue<uint8_t, Traits>."""
    mux = StreamBufferMultiplexer(shared_queue_size=64, traits=SilentTraits)
    q0 = mux.add_queue_producer(0, size=64)
    assert q0.queue.traits == mux.traits
    assert not q0.queue.traits.enable_loss_detection
    mux.close()


def test_adopted_queue_multiplexes_only_what_goes_through_the_handle():
    """Only what is published through the handle is multiplexed: a message put
    on the queue directly is there for anyone reading the queue, but no record
    was fanned in for it."""
    queue = SlickQueue(size=256, element_size=1, items_per_slot=16)

    mux = StreamBufferMultiplexer(shared_queue_size=64, traits=CountingTraits)
    q0 = mux.add_queue_producer(0, queue=queue)
    assert q0.queue is queue
    assert q0.size == 256
    assert q0.items_per_slot == 16

    publish_message(q0, b"via-mux")

    # Straight onto the queue, bypassing the handle. SlickQueue's own [] is one
    # element wide, so the six bytes go in through the handle's view of the ring.
    direct = queue.reserve(6)
    q0[direct][:6] = b"direct"
    queue.publish(direct, 6)

    publish_message(q0, b"again")

    assert drain(mux) == [b"via-mux", b"again"]
    assert mux.loss_count() == 0

    # All three messages are in the queue itself.
    lengths = []
    cursor = 0
    while True:
        data, length, cursor = queue.read(cursor)
        if data is None:
            break
        lengths.append(length)
    assert lengths == [7, 6, 5]
    mux.close()


def test_closing_the_multiplexer_closes_queue_producers():
    mux = StreamBufferMultiplexer(shared_queue_size=64)
    q0 = mux.add_queue_producer(0, size=64)
    mux.close()
    assert q0.queue._buf is None
    assert mux.has_producer(0)  # the table is kept, as for stream buffer producers
    mux.close()                 # closing twice is a no-op


def run_all_tests():
    tests = [
        ("single queue producer roundtrip", test_single_queue_producer_roundtrip),
        ("mixed producers keep publish order", test_mixed_producers_keep_publish_order),
        ("shared atomic cursor reads queue producers",
         test_shared_atomic_cursor_reads_queue_producers),
        ("items_per_slot messages dereference exactly",
         test_items_per_slot_messages_dereference_exactly),
        ("reservation that wraps the ring stays contiguous",
         test_reservation_that_wraps_the_ring_stays_contiguous),
        ("pq[index] view spans the reservation", test_getitem_view_spans_the_reservation),
        ("queue lap causes multiplexer loss", test_queue_lap_causes_multiplexer_loss),
        ("queue lap is skipped with counters off", test_queue_lap_is_skipped_with_counters_off),
        ("inner queue loss_count is not a record count",
         test_inner_queue_loss_count_is_not_a_record_count),
        ("overwritten bytes are not returned", test_overwritten_bytes_are_not_returned),
        ("message in a skipped tail is reported lost",
         test_message_in_a_skipped_tail_is_reported_lost),
        ("duplicate producer_id across kinds raises",
         test_duplicate_producer_id_across_kinds_raises),
        ("invalid queue arguments raise", test_invalid_queue_arguments_raise),
        ("accessors tell the two kinds apart", test_accessors_tell_the_two_kinds_apart),
        ("get_producer_queues() is a live read-only view",
         test_get_producer_queues_is_a_live_read_only_view),
        ("ProducerQueue forwarded API", test_producer_queue_forwarded_api),
        ("queue traits follow the multiplexer", test_queue_traits_follow_the_multiplexer),
        ("adopted queue multiplexes only what goes through the handle",
         test_adopted_queue_multiplexes_only_what_goes_through_the_handle),
        ("closing the multiplexer closes queue producers",
         test_closing_the_multiplexer_closes_queue_producers),
    ]

    print("=" * 70)
    print("Running Multiplexer Queue Producer Tests")
    print("=" * 70)

    passed = 0
    failed = 0
    for name, test_func in tests:
        try:
            print(f"\n[TEST] {name}...", end=" ")
            test_func()
            print("[PASSED]")
            passed += 1
        except Exception as e:
            print("[FAILED]")
            print(f"  Error: {e}")
            import traceback
            traceback.print_exc()
            failed += 1

    print("\n" + "=" * 70)
    print(f"Results: {passed} passed, {failed} failed")
    print("=" * 70)
    return failed == 0


if __name__ == '__main__':
    success = run_all_tests()
    sys.exit(0 if success else 1)
