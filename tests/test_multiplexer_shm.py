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

from slick_stream_buffer_multiplexer_py import (
    StreamBufferMultiplexer,
    ProducerBuffer,
    ProducerQueue,
)


def publish_message(producer, data: bytes) -> None:
    """Publish one message through either kind of producer handle."""
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


def test_shm_unlink_after_close():
    """The documented lifecycle - close every handle, then unlink once - deletes
    every segment. close() used to empty the registration table, and
    SlickQueue.close() drops the handle SlickQueue.unlink() needs, so unlink()
    after close() reached nothing: on POSIX the queue and producer segments all
    outlived the run and were attached, stale, by the next one."""
    q, p0, p1 = "mux_py_uac_records", "mux_py_uac_p0", "mux_py_uac_p1"
    creator = StreamBufferMultiplexer(shared_queue_size=64, name=q)
    creator.add_producer(0, capacity=1024, control_size=16, name=p0)
    creator.add_producer(1, capacity=1024, control_size=16, name=p1)
    opener = StreamBufferMultiplexer(name=q)
    opener.add_producer(0, name=p0)
    opener.add_producer(1, name=p1)

    opener.close()
    creator.close()
    creator.unlink()

    successor = None
    try:
        if sys.platform != 'win32':
            # remove() only succeeds on a name still there (and clears it, so a
            # failure here leaves nothing behind for the next run)
            leaked = [n for n in (q, p0, p1) if StreamBufferMultiplexer.remove(n)]
            assert not leaked, f"segments outlived close() + unlink(): {leaked}"

        # the next run's creators, with new geometry: a stale segment would be
        # attached instead and fail the geometry check
        successor = StreamBufferMultiplexer(shared_queue_size=128, name=q)
        successor.add_producer(0, capacity=2048, control_size=32, name=p0)

        # unlink once: repeating it must not delete the successor's segments
        creator.unlink()
        probe = StreamBufferMultiplexer(name=q)
        try:
            probe.add_producer(0, name=p0)
            assert probe.shared_queue.size == 128
            assert probe.get_producer_buffer(0).capacity == 2048
        finally:
            probe.close()
    finally:
        if successor is not None:
            successor.close()
            successor.unlink()


# ---------------------------------------------------------------------------
# Queue producers in shared memory
# ---------------------------------------------------------------------------

def test_shm_queue_producer_roundtrip():
    """A queue producer's segment is self-describing like a stream buffer's: the
    opener names it and adopts its size and items_per_slot. The 100-byte message
    is longer than items_per_slot on purpose - the opener can only recognize it
    if it rounds with the granularity it adopted."""
    creator = StreamBufferMultiplexer(shared_queue_size=64, name="mux_py_qrt_records")
    opener = None
    try:
        c0 = creator.add_queue_producer(0, size=1024, items_per_slot=64, name="mux_py_qrt_q0")
        c1 = creator.add_producer(1, capacity=1024, control_size=16, name="mux_py_qrt_p1")
        assert c0.use_shm
        assert c0.own_buffer

        opener = StreamBufferMultiplexer(name="mux_py_qrt_records")
        o0 = opener.add_queue_producer(0, name="mux_py_qrt_q0")
        opener.add_producer(1, name="mux_py_qrt_p1")

        assert o0.size == 1024
        assert o0.items_per_slot == 64
        assert o0.use_shm
        assert not o0.own_buffer

        long_message = b"q" * 100
        publish_message(c0, b"hello")
        publish_message(c1, b"world!")
        publish_message(c0, long_message)

        cursor = 0
        for producer_id, data in ((0, b"hello"), (1, b"world!"), (0, long_message)):
            rec, cursor = opener.read(cursor)
            assert rec
            assert rec.producer_id == producer_id
            assert rec.data == data
        rec, cursor = opener.read(cursor)
        assert not rec
        assert opener.loss_count() == 0
    finally:
        if opener is not None:
            opener.close()
        creator.unlink()
        creator.close()


def test_shm_partial_queue_producer_visibility():
    """The partial-visibility rule does not care which kind the unregistered
    producer is."""
    creator = StreamBufferMultiplexer(shared_queue_size=64, name="mux_py_qpart_records")
    opener = None
    try:
        c0 = creator.add_queue_producer(0, size=1024, items_per_slot=16, name="mux_py_qpart_q0")
        c1 = creator.add_queue_producer(1, size=1024, items_per_slot=16)  # local-only

        opener = StreamBufferMultiplexer(name="mux_py_qpart_records")
        opener.add_queue_producer(0, name="mux_py_qpart_q0")
        assert opener.has_producer(0)
        assert not opener.has_producer(1)

        publish_message(c0, b"aaa")
        publish_message(c1, b"bbbb")
        publish_message(c0, b"ccccc")

        cursor = 0
        r0, cursor = opener.read(cursor)
        assert r0 and r0.producer_id == 0 and r0.data == b"aaa"
        # producer 1's record is unregistered on the opener -> silently skipped, not loss
        r1, cursor = opener.read(cursor)
        assert r1 and r1.producer_id == 0 and r1.data == b"ccccc"
        rec, cursor = opener.read(cursor)
        assert not rec
        assert opener.loss_count() == 0
    finally:
        if opener is not None:
            opener.close()
        creator.unlink()
        creator.close()


def test_shm_queue_producer_mismatch_raises():
    creator = StreamBufferMultiplexer(shared_queue_size=64, name="mux_py_qgeo_records")
    other = None
    try:
        creator.add_queue_producer(0, size=1024, items_per_slot=64, name="mux_py_qgeo_q0")

        # a second "creator" over the same name with a different size, then granularity
        other = StreamBufferMultiplexer(shared_queue_size=64, name="mux_py_qgeo_records2")
        for kwargs in (dict(size=2048, items_per_slot=64), dict(size=1024, items_per_slot=16)):
            try:
                other.add_queue_producer(0, name="mux_py_qgeo_q0", **kwargs)
                assert False, f"expected geometry mismatch error for {kwargs}"
            except (ValueError, RuntimeError):
                pass

        # an opener stating a granularity the segment does not have
        try:
            other.add_queue_producer(0, name="mux_py_qgeo_q0", items_per_slot=16)
            assert False, "expected items_per_slot mismatch error"
        except (ValueError, RuntimeError):
            pass

        # an opener with no creator
        try:
            other.add_queue_producer(0, name="mux_py_qgeo_nonexistent_q0")
            assert False, "expected FileNotFoundError"
        except FileNotFoundError:
            pass
        assert not other.has_producer(0)
    finally:
        if other is not None:
            other.unlink()
            other.close()
        creator.unlink()
        creator.close()


def _child_queue_consumer(queue_name: str, q0_name: str, p1_name: str,
                          n_messages: int, result_q):
    """Child process: open the mux, a queue producer and a stream buffer
    producer by name, and read n_messages in global order."""
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
        mux.add_queue_producer(0, name=q0_name)
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


def test_shm_queue_cross_process_roundtrip():
    """A queue producer and a stream buffer producer publish from this process;
    a child process opens everything by name and reads all messages in global
    order."""
    creator = StreamBufferMultiplexer(shared_queue_size=256, name="mux_py_qxp_records")
    try:
        c0 = creator.add_queue_producer(0, size=1 << 12, items_per_slot=16, name="mux_py_qxp_q0")
        c1 = creator.add_producer(1, capacity=1 << 12, control_size=64, name="mux_py_qxp_p1")

        result_q = Queue()
        n = 40
        child = Process(target=_child_queue_consumer,
                        args=("mux_py_qxp_records", "mux_py_qxp_q0", "mux_py_qxp_p1", n, result_q))
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


def test_shm_unlink_after_close_covers_queue_producers():
    """close() then unlink() deletes a queue producer's segment too, once: the
    handle keeps the SharedMemory that SlickQueue.close() drops, as the
    multiplexer does for its record queue."""
    q, q0 = "mux_py_quac_records", "mux_py_quac_q0"
    creator = StreamBufferMultiplexer(shared_queue_size=64, name=q)
    creator.add_queue_producer(0, size=1024, items_per_slot=16, name=q0)
    opener = StreamBufferMultiplexer(name=q)
    opener.add_queue_producer(0, name=q0)

    opener.close()
    creator.close()
    creator.unlink()

    successor = None
    try:
        if sys.platform != 'win32':
            leaked = [n for n in (q, q0) if StreamBufferMultiplexer.remove(n)]
            assert not leaked, f"segments outlived close() + unlink(): {leaked}"

        # the next run's creators, with new geometry: a stale segment would be
        # attached instead and fail the geometry check
        successor = StreamBufferMultiplexer(shared_queue_size=128, name=q)
        successor.add_queue_producer(0, size=2048, items_per_slot=32, name=q0)

        # unlink once: repeating it must not delete the successor's segments
        creator.unlink()
        probe = StreamBufferMultiplexer(name=q)
        try:
            pq = probe.add_queue_producer(0, name=q0)
            assert pq.size == 2048
            assert pq.items_per_slot == 32
        finally:
            probe.close()
    finally:
        if successor is not None:
            successor.close()
            successor.unlink()


# ---------------------------------------------------------------------------
# Segment lifetime: nothing unlinks a segment on its own (C++ v2.1.0+)
# ---------------------------------------------------------------------------

def _remove_all(names) -> None:
    for n in names:
        StreamBufferMultiplexer.remove(n)


def _read_all(q: str, p0: str, q1: str):
    """Open the record queue and both producers by name and read every message."""
    mux = StreamBufferMultiplexer(name=q)
    try:
        mux.add_producer(0, name=p0)
        mux.add_queue_producer(1, name=q1)
        out = []
        cursor = 0
        while True:
            rec, cursor = mux.read(cursor)
            if not rec:
                return out
            out.append((rec.producer_id, bytes(rec.data)))
    finally:
        mux.close()


# A fresh interpreter, so it has its own resource tracker - a multiprocessing
# child would share the parent's, and the parent's exit would be what unlinked
# the names.
_LIFETIME_CHILD = r"""
import sys
from pathlib import Path
root = Path(sys.argv[1])
for p in (root, root.parent / "slick-queue-py", root.parent / "slick-stream-buffer-py"):
    sys.path.insert(0, str(p))
from slick_stream_buffer_multiplexer_py import StreamBufferMultiplexer
q, p0, q1, mode, tag = sys.argv[2:7]
if mode == 'create':
    mux = StreamBufferMultiplexer(shared_queue_size=64, name=q)
    pb = mux.add_producer(0, capacity=1024, control_size=16, name=p0)
    pq = mux.add_queue_producer(1, size=1024, items_per_slot=16, name=q1)
    assert pb.own_buffer and pq.own_buffer, "child expected to create the segments"
else:
    mux = StreamBufferMultiplexer(name=q)
    pb = mux.add_producer(0, name=p0)
    pq = mux.add_queue_producer(1, name=q1)
data = tag.encode()
mv = pb.prepare(len(data))
mv[:] = data
pb.commit(len(data))
pb.consume(len(data))
i = pq.reserve(len(data))
pq[i][:len(data)] = data
pq.publish(i, len(data))
mux.close()
"""


def _run_lifetime_child(q: str, p0: str, q1: str, mode: str, tag: str) -> None:
    import subprocess
    result = subprocess.run(
        [sys.executable, "-c", _LIFETIME_CHILD, str(_root), q, p0, q1, mode, tag],
        capture_output=True, text=True, timeout=60)
    assert result.returncode == 0, f"child failed:\n{result.stdout}\n{result.stderr}"
    # The tracker warns about every registered name it cleans up at shutdown
    assert "resource_tracker" not in result.stderr, result.stderr


def test_shm_unlink_leaves_resource_tracker_alone():
    """Regression: unlink() deleted the record queue and each queue producer's
    segment through SharedMemory.unlink(), which before Python 3.13 also sends
    the resource tracker an UNREGISTER. slick-queue-py keeps no registration
    since 2.3.0, so the tracker raised KeyError and printed a traceback from its
    own process. Routing through SlickQueue.unlink() sends nothing."""
    from multiprocessing import resource_tracker
    names = ("mux_py_ut_records", "mux_py_ut_p0", "mux_py_ut_q1")
    _remove_all(names)
    mux = StreamBufferMultiplexer(shared_queue_size=64, name=names[0])
    mux.add_producer(0, capacity=1024, control_size=16, name=names[1])
    mux.add_queue_producer(1, size=1024, items_per_slot=16, name=names[2])
    mux.close()

    calls = []
    original = resource_tracker.unregister
    resource_tracker.unregister = lambda n, rtype: calls.append((n, rtype))
    try:
        mux.unlink()
    finally:
        resource_tracker.unregister = original
    assert calls == [], calls

    if sys.platform != 'win32':
        leaked = [n for n in names if StreamBufferMultiplexer.remove(n)]
        assert not leaked, f"segments outlived close() + unlink(): {leaked}"


def test_shm_creator_exit_keeps_segments():
    """A creator's exit used to unlink its names on POSIX through the resource
    tracker - the record queue and both kinds of producer segment - stranding
    every consumer still attached. On Windows a section dies with its last
    handle, which no one else holds here."""
    if sys.platform == 'win32':
        return
    names = ("mux_py_cx_records", "mux_py_cx_p0", "mux_py_cx_q1")
    _remove_all(names)
    try:
        _run_lifetime_child(*names, 'create', "from child")
        assert _read_all(*names) == [(0, b"from child"), (1, b"from child")]

        # A restarted creator reattaches to all three and keeps their contents
        again = StreamBufferMultiplexer(shared_queue_size=64, name=names[0])
        try:
            assert not again.shared_queue._own
            pb = again.add_producer(0, capacity=1024, control_size=16, name=names[1])
            pq = again.add_queue_producer(1, size=1024, items_per_slot=16, name=names[2])
            assert not pb.own_buffer
            assert not pq.own_buffer
        finally:
            again.close()
    finally:
        _remove_all(names)


def test_shm_attacher_exit_keeps_segments():
    """Before Python 3.13 attaching registered every name with the attacher's
    resource tracker too, so any peer's exit unlinked the segments on POSIX."""
    names = ("mux_py_ax_records", "mux_py_ax_p0", "mux_py_ax_q1")
    _remove_all(names)
    creator = StreamBufferMultiplexer(shared_queue_size=64, name=names[0])
    try:
        creator.add_producer(0, capacity=1024, control_size=16, name=names[1])
        creator.add_queue_producer(1, size=1024, items_per_slot=16, name=names[2])
        _run_lifetime_child(*names, 'open', "first")
        _run_lifetime_child(*names, 'open', "second")
        assert _read_all(*names) == [
            (0, b"first"), (1, b"first"), (0, b"second"), (1, b"second")]
    finally:
        creator.close()
        creator.unlink()


def run_all_tests():
    tests = [
        ("ShmRoundtrip", test_shm_roundtrip),
        ("ShmPartialProducerVisibility", test_shm_partial_producer_visibility),
        ("ShmDuplicateProducerIdRaises", test_shm_duplicate_producer_id_raises),
        ("ShmGeometryMismatchRaises", test_shm_geometry_mismatch_raises),
        ("ShmOpenerWithoutCreatorRaises", test_shm_opener_without_creator_raises),
        ("ShmCrossProcessRoundtrip", test_shm_cross_process_roundtrip),
        ("ShmUnlinkAfterClose", test_shm_unlink_after_close),
        ("ShmQueueProducerRoundtrip", test_shm_queue_producer_roundtrip),
        ("ShmPartialQueueProducerVisibility", test_shm_partial_queue_producer_visibility),
        ("ShmQueueProducerMismatchRaises", test_shm_queue_producer_mismatch_raises),
        ("ShmQueueCrossProcessRoundtrip", test_shm_queue_cross_process_roundtrip),
        ("ShmUnlinkAfterCloseCoversQueueProducers",
         test_shm_unlink_after_close_covers_queue_producers),
        ("ShmUnlinkLeavesResourceTrackerAlone",
         test_shm_unlink_leaves_resource_tracker_alone),
        ("ShmCreatorExitKeepsSegments", test_shm_creator_exit_keeps_segments),
        ("ShmAttacherExitKeepsSegments", test_shm_attacher_exit_keeps_segments),
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
