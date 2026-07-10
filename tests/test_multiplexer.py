"""
Basic functionality tests for StreamBufferMultiplexer (local memory mode).

Mirrors slick-stream-buffer-multiplexer/tests/multiplexer_tests.cpp one-for-one
(adapted to the Python API: read() returns (record, cursor), prepare() returns
a writable memoryview, data payloads are bytes copies).
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
    MultiplexRecord,
    AtomicCursor,
)
from slick_stream_buffer_py import SlickStreamBuffer


def publish_message(pb: ProducerBuffer, data: bytes) -> None:
    """prepare + write + commit + consume in one step."""
    mv = pb.prepare(len(data))
    assert len(mv) == len(data)
    mv[:] = data
    pb.commit(len(data))
    pb.consume(len(data))


def test_empty_multiplexer_read_returns_falsy():
    mux = StreamBufferMultiplexer(shared_queue_size=64)

    assert mux.producer_count() == 0
    assert mux.initial_reading_index() == 0
    assert mux.loss_count() == 0

    rec, cursor = mux.read(0)
    assert not rec
    assert rec.data is None
    assert rec.length == 0
    assert cursor == 0

    cursor_buf = bytearray(8)
    shared_cursor = AtomicCursor(cursor_buf, 0)
    shared_cursor.store(0)
    rec2, _ = mux.read(shared_cursor)
    assert not rec2
    shared_cursor.release()
    mux.close()


def test_single_producer_roundtrip():
    mux = StreamBufferMultiplexer(shared_queue_size=64)
    p0 = mux.add_producer(0, capacity=1024, control_size=16)
    assert p0.producer_id == 0

    publish_message(p0, b"hello")

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


def test_multiple_producers_non_contiguous_ids():
    mux = StreamBufferMultiplexer(shared_queue_size=64)
    p0 = mux.add_producer(0, capacity=1024, control_size=16)
    p2 = mux.add_producer(2, capacity=2048, control_size=32)

    assert mux.producer_count() == 2
    assert mux.has_producer(0)
    assert not mux.has_producer(1)
    assert mux.has_producer(2)

    publish_message(p0, b"aaa")
    publish_message(p2, b"bbbb")
    publish_message(p0, b"ccccc")

    cursor = 0

    r0, cursor = mux.read(cursor)
    assert r0
    assert r0.producer_id == 0
    assert r0.length == 3
    assert r0.data == b"aaa"

    r1, cursor = mux.read(cursor)
    assert r1
    assert r1.producer_id == 2
    assert r1.length == 4
    assert r1.data == b"bbbb"

    r2, cursor = mux.read(cursor)
    assert r2
    assert r2.producer_id == 0
    assert r2.length == 5
    assert r2.data == b"ccccc"

    rec, cursor = mux.read(cursor)
    assert not rec
    assert mux.loss_count() == 0
    mux.close()


def test_consume_zero_publishes_nothing():
    mux = StreamBufferMultiplexer(shared_queue_size=64)
    p0 = mux.add_producer(0, capacity=1024, control_size=16)

    mv = p0.prepare(3)
    mv[:] = b"abc"
    p0.commit(3)

    rec = p0.consume(0)
    assert not bool(rec)

    result, cursor = mux.read(0)
    assert not result
    mux.close()


def test_unregistered_producer_queries():
    mux = StreamBufferMultiplexer(shared_queue_size=64)
    mux.add_producer(0, capacity=1024, control_size=16)

    assert not mux.has_producer(5)
    assert mux.get_producer_buffer(5) is None
    assert mux.find_producer(5) is None
    mux.close()


def test_duplicate_producer_id_raises():
    mux = StreamBufferMultiplexer(shared_queue_size=64)
    mux.add_producer(0, capacity=1024, control_size=16)
    try:
        mux.add_producer(0, capacity=2048, control_size=32)
        assert False, "expected ValueError"
    except ValueError as e:
        assert "already registered" in str(e)
    mux.close()


def test_invalid_sizes_raise():
    for size in (0, 100):  # shared queue size not pow2
        try:
            StreamBufferMultiplexer(shared_queue_size=size)
            assert False, f"expected ValueError for shared_queue_size={size}"
        except ValueError:
            pass

    mux = StreamBufferMultiplexer(shared_queue_size=64)
    for kwargs in (dict(capacity=1000, control_size=16),   # capacity not pow2
                   dict(capacity=1024, control_size=15)):  # control_size not pow2
        try:
            mux.add_producer(0, **kwargs)
            assert False, f"expected ValueError for {kwargs}"
        except ValueError:
            pass
    mux.close()


def test_producer_buffer_forwarded_api():
    mux = StreamBufferMultiplexer(shared_queue_size=64)
    p0 = mux.add_producer(0, capacity=1024, control_size=16)

    assert p0.capacity == 1024
    assert p0.control_size == 16
    assert p0.loss_count() == 0
    assert p0.initial_reading_index() == 0
    assert p0.own_buffer
    assert not p0.use_shm

    mv = p0.prepare(5)
    mv[:] = b"hello"
    p0.commit(5)
    assert p0.size() == 5
    assert bytes(p0.data()) == b"hello"

    p0.discard()
    assert p0.size() == 0
    mux.close()


def test_stream_buffer_accessor():
    """Python merges C++'s stream_buffer() and stream_buffer_ptr() into one
    property; it must expose the actual underlying SlickStreamBuffer."""
    mux = StreamBufferMultiplexer(shared_queue_size=64)
    p0 = mux.add_producer(0, capacity=1024, control_size=16)

    sb = p0.stream_buffer
    assert isinstance(sb, SlickStreamBuffer)
    assert sb is p0.stream_buffer  # stable identity (shared_ptr semantics)

    publish_message(p0, b"x")

    data, length, cursor = sb.read(0)
    assert data == b"x"
    assert length == 1
    mux.close()


def run_all_tests():
    tests = [
        ("EmptyMultiplexerReadReturnsFalsy", test_empty_multiplexer_read_returns_falsy),
        ("SingleProducerRoundtrip", test_single_producer_roundtrip),
        ("MultipleProducersNonContiguousIds", test_multiple_producers_non_contiguous_ids),
        ("ConsumeZeroPublishesNothing", test_consume_zero_publishes_nothing),
        ("UnregisteredProducerQueries", test_unregistered_producer_queries),
        ("DuplicateProducerIdRaises", test_duplicate_producer_id_raises),
        ("InvalidSizesRaise", test_invalid_sizes_raise),
        ("ProducerBufferForwardedApi", test_producer_buffer_forwarded_api),
        ("StreamBufferAccessor", test_stream_buffer_accessor),
    ]

    print("=" * 70)
    print("Running Multiplexer Basic Tests")
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
