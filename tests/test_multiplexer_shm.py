"""
Shared memory tests for StreamBufferMultiplexer.

Mirrors slick-stream-buffer-multiplexer/tests/multiplexer_shm_tests.cpp,
plus a cross-process roundtrip via multiprocessing.
"""
import sys
import multiprocessing
from multiprocessing import Process, Queue
from pathlib import Path

if __name__ == '__main__':
    try:
        multiprocessing.set_start_method('spawn', force=False)
    except RuntimeError:
        pass

_root = Path(__file__).parent.parent
sys.path.insert(0, str(_root))
sys.path.insert(0, str(_root.parent / "slick-queue-py"))
sys.path.insert(0, str(_root.parent / "slick-stream-buffer-py"))

from slick_stream_buffer_multiplexer_py import StreamBufferMultiplexer, ProducerBuffer


def publish_message(pb: ProducerBuffer, data: bytes) -> None:
    mv = pb.prepare(len(data))
    mv[:] = data
    pb.commit(len(data))
    pb.consume(len(data))


def _cleanup(*muxes):
    for m in muxes:
        try:
            m.close()
        except Exception:
            pass
    # unlink via the creator (first argument)
    # note: close() clears the producer map, so unlink the raw segments instead
    # by re-listing names is unnecessary here - tests unlink before close below


def test_shm_roundtrip():
    creator = StreamBufferMultiplexer(shared_queue_size=64, name="mux_py_rt_records")
    c0 = creator.add_producer(0, capacity=1024, control_size=16, name="mux_py_rt_p0")
    c1 = creator.add_producer(1, capacity=2048, control_size=32, name="mux_py_rt_p1")

    opener = StreamBufferMultiplexer(name="mux_py_rt_records")
    try:
        o0 = opener.add_producer(0, name="mux_py_rt_p0")
        o1 = opener.add_producer(1, name="mux_py_rt_p1")

        assert o0.capacity == 1024
        assert o0.control_size == 16
        assert o0.use_shm
        assert not o0.own_buffer

        assert o1.capacity == 2048
        assert o1.control_size == 32

        publish_message(c0, b"hello")
        publish_message(c1, b"world!")

        cursor = 0
        r0, cursor = opener.read(cursor)
        assert r0
        assert r0.producer_id == 0
        assert r0.length == 5
        assert r0.data == b"hello"

        r1, cursor = opener.read(cursor)
        assert r1
        assert r1.producer_id == 1
        assert r1.length == 6
        assert r1.data == b"world!"

        rec, cursor = opener.read(cursor)
        assert not rec
        assert opener.loss_count() == 0
    finally:
        opener.close()
        creator.unlink()
        creator.close()


def test_shm_partial_producer_visibility():
    creator = StreamBufferMultiplexer(shared_queue_size=64, name="mux_py_part_records")
    c0 = creator.add_producer(0, capacity=1024, control_size=16, name="mux_py_part_p0")
    c1 = creator.add_producer(1, capacity=2048, control_size=32)  # local-only

    opener = StreamBufferMultiplexer(name="mux_py_part_records")
    try:
        opener.add_producer(0, name="mux_py_part_p0")
        assert opener.has_producer(0)
        assert not opener.has_producer(1)

        publish_message(c0, b"aaa")
        publish_message(c1, b"bbbb")
        publish_message(c0, b"ccccc")

        cursor = 0

        r0, cursor = opener.read(cursor)
        assert r0
        assert r0.producer_id == 0
        assert r0.length == 3
        assert r0.data == b"aaa"

        # producer 1's record is unregistered on the opener -> silently skipped, not loss
        r1, cursor = opener.read(cursor)
        assert r1
        assert r1.producer_id == 0
        assert r1.length == 5
        assert r1.data == b"ccccc"

        rec, cursor = opener.read(cursor)
        assert not rec
        assert opener.loss_count() == 0
    finally:
        opener.close()
        creator.unlink()
        creator.close()


def test_shm_duplicate_producer_id_raises():
    mux = StreamBufferMultiplexer(shared_queue_size=64, name="mux_py_dup_records")
    try:
        mux.add_producer(0, capacity=1024, control_size=16, name="mux_py_dup_p0")

        for kwargs in (dict(capacity=2048, control_size=32),
                       dict(name="mux_py_dup_p0")):
            try:
                mux.add_producer(0, **kwargs)
                assert False, f"expected ValueError for {kwargs}"
            except ValueError:
                pass
    finally:
        mux.unlink()
        mux.close()


def test_shm_geometry_mismatch_raises():
    creator1 = StreamBufferMultiplexer(shared_queue_size=64, name="mux_py_geo_records")
    creator2 = None
    try:
        creator1.add_producer(0, capacity=1024, control_size=16, name="mux_py_geo_p0")

        # a second "creator" with the same shared-queue name but different size
        try:
            StreamBufferMultiplexer(shared_queue_size=128, name="mux_py_geo_records")
            assert False, "expected geometry mismatch error"
        except (ValueError, RuntimeError):
            pass

        # a second "creator" producer with the same shm name but different geometry
        creator2 = StreamBufferMultiplexer(shared_queue_size=64, name="mux_py_geo_records2")
        try:
            creator2.add_producer(0, capacity=2048, control_size=16, name="mux_py_geo_p0")
            assert False, "expected geometry mismatch error"
        except (ValueError, RuntimeError):
            pass
    finally:
        if creator2 is not None:
            creator2.unlink()
            creator2.close()
        creator1.unlink()
        creator1.close()


def test_shm_opener_without_creator_raises():
    try:
        StreamBufferMultiplexer(name="mux_py_nonexistent_records")
        assert False, "expected FileNotFoundError"
    except FileNotFoundError:
        pass

    mux = StreamBufferMultiplexer(shared_queue_size=64, name="mux_py_noprod_records")
    try:
        try:
            mux.add_producer(0, name="mux_py_nonexistent_p0")
            assert False, "expected FileNotFoundError"
        except FileNotFoundError:
            pass
    finally:
        mux.unlink()
        mux.close()


def _child_consumer(queue_name: str, p0_name: str, p1_name: str,
                    n_messages: int, result_q):
    """Child process: open the mux + producers, read n_messages in global order."""
    import sys as _sys
    import time as _time
    from pathlib import Path as _Path
    _r = _Path(__file__).parent.parent
    _sys.path.insert(0, str(_r))
    _sys.path.insert(0, str(_r.parent / "slick-queue-py"))
    _sys.path.insert(0, str(_r.parent / "slick-stream-buffer-py"))
    from slick_stream_buffer_multiplexer_py import StreamBufferMultiplexer as _Mux

    mux = _Mux(name=queue_name)
    try:
        mux.add_producer(0, name=p0_name)
        mux.add_producer(1, name=p1_name)

        cursor = 0
        received = []
        deadline = _time.time() + 60
        while len(received) < n_messages:
            if _time.time() > deadline:
                result_q.put(('timeout', received))
                return
            rec, cursor = mux.read(cursor)
            if not rec:
                _time.sleep(0)
                continue
            received.append((rec.producer_id, bytes(rec.data)))
        result_q.put(('ok', received))
    finally:
        mux.close()


def test_shm_cross_process_roundtrip():
    """Creator publishes from this process; a child process opens everything
    by name and reads all messages in global order."""
    creator = StreamBufferMultiplexer(shared_queue_size=256, name="mux_py_xp_records")
    try:
        c0 = creator.add_producer(0, capacity=1 << 12, control_size=64, name="mux_py_xp_p0")
        c1 = creator.add_producer(1, capacity=1 << 12, control_size=64, name="mux_py_xp_p1")

        result_q = Queue()
        n = 40
        child = Process(target=_child_consumer,
                        args=("mux_py_xp_records", "mux_py_xp_p0", "mux_py_xp_p1", n, result_q))
        child.start()

        expected = []
        for i in range(n):
            pid = i % 2
            data = f"m{i:03d}".encode()
            publish_message(c0 if pid == 0 else c1, data)
            expected.append((pid, data))

        status, received = result_q.get(timeout=90)
        child.join(timeout=30)
        assert status == 'ok', f"child failed: {status} after {len(received)} messages"
        assert received == expected
    finally:
        creator.unlink()
        creator.close()


def run_all_tests():
    tests = [
        ("ShmRoundtrip", test_shm_roundtrip),
        ("ShmPartialProducerVisibility", test_shm_partial_producer_visibility),
        ("ShmDuplicateProducerIdRaises", test_shm_duplicate_producer_id_raises),
        ("ShmGeometryMismatchRaises", test_shm_geometry_mismatch_raises),
        ("ShmOpenerWithoutCreatorRaises", test_shm_opener_without_creator_raises),
        ("ShmCrossProcessRoundtrip", test_shm_cross_process_roundtrip),
    ]

    print("=" * 70)
    print("Running Multiplexer Shared Memory Tests")
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
    multiprocessing.freeze_support()
    success = run_all_tests()
    sys.exit(0 if success else 1)
