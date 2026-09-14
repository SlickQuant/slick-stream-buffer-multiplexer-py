"""
Multi-producer / multi-consumer stress tests for StreamBufferMultiplexer.

Mirrors slick-stream-buffer-multiplexer/tests/multiplexer_mpmc_tests.cpp
(threads instead of std::thread; payload is packed "<IQ" producer_id+seq).
"""
import struct
import sys
import threading
import time
from pathlib import Path

_root = Path(__file__).parent.parent
sys.path.insert(0, str(_root))
sys.path.insert(0, str(_root.parent / "slick-queue-py"))
sys.path.insert(0, str(_root.parent / "slick-stream-buffer-py"))

from slick_stream_buffer_multiplexer_py import (
    StreamBufferMultiplexer,
    AtomicCursor,
    QueueTraits,
)


class SilentTraits(QueueTraits):
    """Both loss terms off - records are still skipped, only the counters go quiet."""
    enable_loss_detection = False

PAYLOAD_FMT = "<IQ"  # producer_id u32, seq u64
PAYLOAD_SIZE = struct.calcsize(PAYLOAD_FMT)


def test_multi_threaded_broadcast():
    k_producers = 4
    k_messages_per_producer = 200
    k_consumers = 3
    k_total = k_producers * k_messages_per_producer

    # all add_producer calls happen here, single-threaded, before threads start
    mux = StreamBufferMultiplexer(shared_queue_size=1024)
    for pid in range(k_producers):
        mux.add_producer(pid, capacity=1 << 16, control_size=256)

    def producer(pid: int):
        pb = mux.get_producer_buffer(pid)
        for seq in range(k_messages_per_producer):
            data = struct.pack(PAYLOAD_FMT, pid, seq)
            mv = pb.prepare(len(data))
            mv[:] = data
            pb.commit(len(data))
            pb.consume(len(data))

    ok = [True] * k_consumers
    received = [0] * k_consumers

    def consumer(c: int):
        cursor = 0
        expected = [0] * k_producers
        count = 0
        while count < k_total:
            rec, cursor = mux.read(cursor)
            if not rec:
                time.sleep(0)
                continue
            if rec.length != PAYLOAD_SIZE or rec.producer_id >= k_producers:
                ok[c] = False
                break
            data_pid, data_seq = struct.unpack(PAYLOAD_FMT, rec.data)
            if data_pid != rec.producer_id or data_seq != expected[rec.producer_id]:
                ok[c] = False
                break
            expected[rec.producer_id] += 1
            count += 1
        received[c] = count

    producer_threads = [threading.Thread(target=producer, args=(pid,))
                        for pid in range(k_producers)]
    consumer_threads = [threading.Thread(target=consumer, args=(c,))
                        for c in range(k_consumers)]
    for t in producer_threads + consumer_threads:
        t.start()
    for t in producer_threads:
        t.join(timeout=120)
        assert not t.is_alive(), "producer thread timed out"
    for t in consumer_threads:
        t.join(timeout=120)
        assert not t.is_alive(), "consumer thread timed out"

    for c in range(k_consumers):
        assert ok[c], f"consumer {c} observed unexpected data"
        assert received[c] == k_total
    assert mux.loss_count() == 0
    for pid in range(k_producers):
        assert mux.get_producer_buffer(pid).loss_count() == 0
    mux.close()


def test_work_stealing_shared_cursor():
    k_producers = 3
    k_messages_per_producer = 150
    k_consumers = 4
    k_total = k_producers * k_messages_per_producer

    mux = StreamBufferMultiplexer(shared_queue_size=1024)
    for pid in range(k_producers):
        mux.add_producer(pid, capacity=1 << 16, control_size=256)

    def producer(pid: int):
        pb = mux.get_producer_buffer(pid)
        for seq in range(k_messages_per_producer):
            data = struct.pack(PAYLOAD_FMT, pid, seq)
            mv = pb.prepare(len(data))
            mv[:] = data
            pb.commit(len(data))
            pb.consume(len(data))

    producer_threads = [threading.Thread(target=producer, args=(pid,))
                        for pid in range(k_producers)]
    for t in producer_threads:
        t.start()
    for t in producer_threads:
        t.join(timeout=120)
        assert not t.is_alive()

    # all messages are published; consumers drain the queue via one shared
    # atomic cursor - each message must be delivered to exactly one consumer
    cursor_buf = bytearray(8)
    shared_cursor = AtomicCursor(cursor_buf, 0)
    shared_cursor.store(0)
    seen = [[] for _ in range(k_consumers)]

    def consumer(c: int):
        while True:
            rec, _ = mux.read(shared_cursor)
            if not rec:
                break  # queue fully drained (producers already finished)
            assert rec.length == PAYLOAD_SIZE
            data_pid, data_seq = struct.unpack(PAYLOAD_FMT, rec.data)
            seen[c].append(data_pid * k_messages_per_producer + data_seq)

    consumer_threads = [threading.Thread(target=consumer, args=(c,))
                        for c in range(k_consumers)]
    for t in consumer_threads:
        t.start()
    for t in consumer_threads:
        t.join(timeout=120)
        assert not t.is_alive()

    union_count = [0] * k_total
    total = 0
    for v in seen:
        for idx in v:
            union_count[idx] += 1
            total += 1
    assert total == k_total, f"expected {k_total} deliveries, got {total}"
    for i in range(k_total):
        assert union_count[i] == 1, f"global index {i} delivered {union_count[i]} times"
    assert mux.loss_count() == 0
    shared_cursor.release()
    mux.close()


def test_shared_queue_wrap_causes_loss():
    mux = StreamBufferMultiplexer(shared_queue_size=4)      # tiny shared queue
    p0 = mux.add_producer(0, capacity=1 << 16, control_size=256)  # no inner lap

    for i in range(10):
        mv = p0.prepare(1)
        mv[:] = bytes([i])
        p0.commit(1)
        p0.consume(1)

    rec, cursor = mux.read(0)
    assert rec
    assert rec.producer_id == 0
    assert rec.length == 1
    assert rec.data[0] == 8  # shared queue of size 4: slot 0 was last written by record 8
    assert mux.loss_count() == 8
    mux.close()


def test_producer_ring_lap_causes_multiplexer_loss():
    mux = StreamBufferMultiplexer(shared_queue_size=64)  # no shared-queue loss
    p0 = mux.add_producer(0, capacity=1024, control_size=4)  # laps after 4 records

    for i in range(10):
        mv = p0.prepare(1)
        mv[:] = bytes([i])
        p0.commit(1)
        p0.consume(1)

    cursor = 0
    values = []
    while True:
        rec, cursor = mux.read(cursor)
        if not rec:
            break
        assert rec.length == 1
        values.append(rec.data[0])

    # only the last 4 published records (6..9) survive in the producer's control ring
    assert values == [6, 7, 8, 9], f"got {values}"
    assert mux.loss_count() == 6
    # The multiplexer dereferences through traits that do not count, so the
    # producer's own inner-ring counter must stay untouched: it is reserved for
    # reads the caller makes directly through stream_buffer.read().
    assert p0.loss_count() == 0, \
        f"dereference must not move the producer's loss counter, got {p0.loss_count()}"
    mux.close()


def test_traits_disable_loss_counting():
    """enable_loss_detection switches BOTH terms of loss_count() together. Records
    are skipped exactly as before; only the counters are silent."""
    mux = StreamBufferMultiplexer(shared_queue_size=64, traits=SilentTraits)
    assert mux.traits.enable_loss_detection is False
    p0 = mux.add_producer(0, capacity=1024, control_size=4)  # laps after 4 records

    for i in range(10):
        mv = p0.prepare(1)
        mv[:] = bytes([i])
        p0.commit(1)
        p0.consume(1)

    cursor = 0
    values = []
    while True:
        rec, cursor = mux.read(cursor)
        if not rec:
            break
        values.append(rec.data[0])

    assert values == [6, 7, 8, 9], f"got {values}"
    assert mux.loss_count() == 0, "silent traits must not accumulate loss_count()"
    assert p0.loss_count() == 0
    mux.close()


def test_scan_loss_counter_is_a_different_number_from_multiplexer_loss():
    """Why the dereference does not use count_loss.

    read()'s counter answers "how far has the producer run past this cursor?",
    which for a sequential scan is the number of records skipped. The
    multiplexer counts one loss per lapped record instead. On the same run the
    two are legitimately different numbers - here 8 against 6 - and the scan
    figure is the wrong one for a jump-read, because every later lapped
    dereference would add its own copy of that same distance all over again.
    """
    from slick_stream_buffer_py import counting_read_traits

    mux = StreamBufferMultiplexer(shared_queue_size=64)
    p0 = mux.add_producer(0, capacity=1024, control_size=4)

    for i in range(10):
        mv = p0.prepare(1)
        mv[:] = bytes([i])
        p0.commit(1)
        p0.consume(1)

    # The multiplexer's own reads leave the producer's counter alone.
    cursor = 0
    while True:
        rec, cursor = mux.read(cursor)
        if not rec:
            break
    assert mux.loss_count() == 6, "one count per lapped record"
    assert p0.loss_count() == 0, "the dereference never touches this counter"

    # A caller who reads that ring directly with counting traits gets the scan
    # figure: slot 0 now holds seq 8, so a cursor at 0 jumps 8 sequences.
    data, _, _ = p0.stream_buffer.read(0, counting_read_traits)
    assert data is not None
    assert p0.loss_count() == 8,         f"the scan counter measures cursor distance, not lapped records: {p0.loss_count()}"
    mux.close()


def run_all_tests():
    tests = [
        ("MultiThreadedBroadcast", test_multi_threaded_broadcast),
        ("WorkStealingSharedCursor", test_work_stealing_shared_cursor),
        ("SharedQueueWrapCausesLoss", test_shared_queue_wrap_causes_loss),
        ("ProducerRingLapCausesMultiplexerLoss", test_producer_ring_lap_causes_multiplexer_loss),
        ("TraitsDisableLossCounting", test_traits_disable_loss_counting),
        ("ScanLossCounterIsADifferentNumberFromMultiplexerLoss",
         test_scan_loss_counter_is_a_different_number_from_multiplexer_loss),
    ]

    print("=" * 70)
    print("Running Multiplexer MPMC Tests")
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
