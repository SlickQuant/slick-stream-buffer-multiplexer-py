"""
C++/Python interoperability tests for StreamBufferMultiplexer.

Verifies binary compatibility by fanning messages through the same shared
record queue and producer segments between real C++ processes (built against
the actual stream_buffer_multiplexer.hpp) and this Python implementation.

The framed message format must match tests/interop_common.hpp exactly:
    u32 worker_id (LE, == producer_id here) | u32 msg_num (LE) | payload
    total length = 8 + (msg_num * 7) % 57
    payload fill = (msg_num * 31 + 7) & 0xFF

Requires the C++ test binaries to be built first:
    cmake -S . -B build && cmake --build build --config Debug
"""
import argparse
import struct
import subprocess
import sys
import time
from pathlib import Path

_root = Path(__file__).parent.parent
sys.path.insert(0, str(_root))
sys.path.insert(0, str(_root.parent / "slick-queue-py"))
sys.path.insert(0, str(_root.parent / "slick-stream-buffer-py"))

from slick_stream_buffer_multiplexer_py import (
    StreamBufferMultiplexer,
    ProducerBuffer,
    ProducerQueue,
    QueueTraits,
)


# ---------------------------------------------------------------------------
# Message framing (must match interop_common.hpp)
# ---------------------------------------------------------------------------

def interop_msg_length(msg_num: int) -> int:
    return 8 + (msg_num * 7) % 57


def interop_fill_byte(msg_num: int) -> int:
    return (msg_num * 31 + 7) & 0xFF


def interop_make_msg(worker_id: int, msg_num: int) -> bytes:
    length = interop_msg_length(msg_num)
    return struct.pack("<II", worker_id, msg_num) + bytes([interop_fill_byte(msg_num)]) * (length - 8)


def interop_check_msg(data: bytes, expected_worker: int) -> int:
    """Validate a received message; returns msg_num or raises AssertionError."""
    assert len(data) >= 8, f"message too short: {len(data)}"
    worker_id, msg_num = struct.unpack_from("<II", data, 0)
    assert worker_id == expected_worker, f"worker_id {worker_id} != {expected_worker}"
    assert len(data) == interop_msg_length(msg_num), \
        f"length {len(data)} != expected {interop_msg_length(msg_num)} for msg {msg_num}"
    fill = interop_fill_byte(msg_num)
    assert all(b == fill for b in data[8:]), f"payload corrupted in msg {msg_num}"
    return msg_num


def publish_msg(producer, msg_num: int) -> None:
    """Publish one framed message through either kind of producer handle."""
    data = interop_make_msg(producer.producer_id, msg_num)
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


# ---------------------------------------------------------------------------
# Harness
# ---------------------------------------------------------------------------

def find_cpp_executable(name: str) -> str:
    """Find a C++ test executable in the CMake build directory."""
    search_paths = [
        _root / "build" / name,
        _root / "build" / "tests" / name,
        _root / "build" / f"{name}.exe",
        _root / "build" / "tests" / "Debug" / f"{name}.exe",
        _root / "build" / "tests" / "Release" / f"{name}.exe",
        _root / "build" / "tests" / f"{name}.exe",
    ]
    for path in search_paths:
        if path.exists():
            return str(path)
    raise FileNotFoundError(
        f"C++ executable '{name}' not found. Build the project first: "
        f"cmake -S . -B build && cmake --build build --config Debug")


def python_read_all(mux: StreamBufferMultiplexer, totals: dict,
                    timeout_s: float = 30.0) -> None:
    """Read messages until every producer in `totals` delivered its count.
    Per-producer msg_num sequences must be strictly sequential from 0."""
    expected = {pid: 0 for pid in totals}
    remaining = sum(totals.values())
    cursor = 0
    deadline = time.time() + timeout_s
    while remaining > 0:
        assert time.time() < deadline, \
            f"timed out; progress {expected} of {totals}"
        rec, cursor = mux.read(cursor)
        if not rec:
            time.sleep(0)
            continue
        assert rec.producer_id in expected, f"unexpected producer {rec.producer_id}"
        msg_num = interop_check_msg(rec.data, rec.producer_id)
        assert msg_num == expected[rec.producer_id], \
            f"producer {rec.producer_id}: expected msg {expected[rec.producer_id]}, got {msg_num}"
        expected[rec.producer_id] += 1
        remaining -= 1
    assert expected == totals


def _finish(proc: subprocess.Popen) -> None:
    """Release a --hold producer and wait for it."""
    try:
        proc.stdin.write("exit\n")
        proc.stdin.flush()
    except Exception:
        pass
    try:
        proc.wait(timeout=30)
    except subprocess.TimeoutExpired:
        proc.kill()


# ---------------------------------------------------------------------------
# Tests
# ---------------------------------------------------------------------------

def test_python_producers_cpp_consumer():
    """Python creates the queue + 2 shm producers and publishes; a C++
    consumer opens everything and verifies global order and content."""
    print("TEST: Python producers -> C++ consumer")
    n = 50  # per producer
    mux = StreamBufferMultiplexer(shared_queue_size=1024, name="mux_io_p2c_records")
    try:
        p0 = mux.add_producer(0, capacity=1 << 16, control_size=256, name="mux_io_p2c_p0")
        p1 = mux.add_producer(1, capacity=1 << 16, control_size=256, name="mux_io_p2c_p1")

        consumer = subprocess.Popen(
            [find_cpp_executable("cpp_mux_consumer"), mux.get_shm_name(), str(2 * n),
             "--producer", "0", p0.get_shm_name(),
             "--producer", "1", p1.get_shm_name()],
            stdout=subprocess.PIPE, stderr=subprocess.PIPE, text=True)

        for i in range(n):  # interleave the two producers
            publish_msg(p0, i)
            publish_msg(p1, i)

        out, err = consumer.communicate(timeout=60)
        assert consumer.returncode == 0, f"cpp_mux_consumer failed (rc={consumer.returncode}): {err}"
        assert f"CONSUMED {2 * n}" in out, f"unexpected output: {out}"
        assert f"PRODUCER 0 {n}" in out and f"PRODUCER 1 {n}" in out
        print(f"  C++ consumed all {2 * n} messages from 2 Python producers")
    finally:
        mux.unlink()
        mux.close()


def test_cpp_producers_python_consumer():
    """Python creates the queue; two concurrent C++ producer processes create
    their own segments and fan in (cross-process MPMC reserve on the shared
    queue); Python consumes everything."""
    print("TEST: 2 C++ producer processes -> Python consumer")
    n = 50  # per producer
    mux = StreamBufferMultiplexer(shared_queue_size=1024, name="mux_io_c2p_records")
    producers = []
    try:
        for pid, seg in ((0, "mux_io_c2p_p0"), (1, "mux_io_c2p_p1")):
            proc = subprocess.Popen(
                [find_cpp_executable("cpp_mux_producer"), mux.get_shm_name(), seg,
                 str(pid), str(n), "create-producer", str(1 << 16), "256", "--hold"],
                stdin=subprocess.PIPE, stdout=subprocess.PIPE, stderr=subprocess.PIPE,
                text=True)
            producers.append(proc)

        # wait for both to create their segments before registering
        for proc in producers:
            line = proc.stdout.readline()
            assert "READY" in line, f"unexpected producer output: {line!r}"

        mux.add_producer(0, name="mux_io_c2p_p0")
        mux.add_producer(1, name="mux_io_c2p_p1")

        python_read_all(mux, {0: n, 1: n})
        print(f"  Python consumed all {2 * n} messages from 2 concurrent C++ producers")

        for proc in producers:
            line = proc.stdout.readline()
            assert f"PUBLISHED {n}" in line, f"unexpected producer output: {line!r}"
    finally:
        for proc in producers:
            _finish(proc)
        mux.unlink()
        mux.close()


def test_mixed_language_producers():
    """One Python producer and one C++ producer fan into the same shared
    queue concurrently; consumers in BOTH languages verify the result."""
    print("TEST: mixed Python + C++ producers, consumers in both languages")
    n = 50  # per producer
    mux = StreamBufferMultiplexer(shared_queue_size=1024, name="mux_io_mix_records")
    cpp_producer = None
    cpp_consumer = None
    try:
        p0 = mux.add_producer(0, capacity=1 << 16, control_size=256, name="mux_io_mix_p0")

        cpp_producer = subprocess.Popen(
            [find_cpp_executable("cpp_mux_producer"), mux.get_shm_name(), "mux_io_mix_p1",
             "1", str(n), "create-producer", str(1 << 16), "256", "--hold"],
            stdin=subprocess.PIPE, stdout=subprocess.PIPE, stderr=subprocess.PIPE, text=True)
        line = cpp_producer.stdout.readline()
        assert "READY" in line, f"unexpected producer output: {line!r}"
        mux.add_producer(1, name="mux_io_mix_p1")

        # C++ consumer watches the same segments concurrently
        cpp_consumer = subprocess.Popen(
            [find_cpp_executable("cpp_mux_consumer"), mux.get_shm_name(), str(2 * n),
             "--producer", "0", p0.get_shm_name(),
             "--producer", "1", "mux_io_mix_p1"],
            stdout=subprocess.PIPE, stderr=subprocess.PIPE, text=True)

        # Python producer publishes concurrently with the C++ producer
        for i in range(n):
            publish_msg(p0, i)

        # Python consumer reads the interleaved global stream
        python_read_all(mux, {0: n, 1: n})
        print(f"  Python consumer saw all {2 * n} messages from both languages")

        out, err = cpp_consumer.communicate(timeout=60)
        assert cpp_consumer.returncode == 0, f"cpp_mux_consumer failed: {err}"
        assert f"CONSUMED {2 * n}" in out
        print(f"  C++ consumer saw all {2 * n} messages from both languages")

        line = cpp_producer.stdout.readline()
        assert f"PUBLISHED {n}" in line
    finally:
        if cpp_producer is not None:
            _finish(cpp_producer)
        if cpp_consumer is not None and cpp_consumer.poll() is None:
            cpp_consumer.kill()
        mux.unlink()
        mux.close()


def test_partial_registration_interop():
    """A C++ consumer that registers only one of two Python producers must
    silently skip the other's records (no loss, no error)."""
    print("TEST: partial registration across languages")
    n = 30
    mux = StreamBufferMultiplexer(shared_queue_size=1024, name="mux_io_part_records")
    try:
        p0 = mux.add_producer(0, capacity=1 << 16, control_size=256, name="mux_io_part_p0")
        p1 = mux.add_producer(1, capacity=1 << 16, control_size=256, name="mux_io_part_p1")

        for i in range(n):
            publish_msg(p0, i)
            publish_msg(p1, i)

        # consumer only registers producer 0 - expects exactly n messages
        result = subprocess.run(
            [find_cpp_executable("cpp_mux_consumer"), mux.get_shm_name(), str(n),
             "--producer", "0", p0.get_shm_name()],
            capture_output=True, text=True, timeout=60)
        assert result.returncode == 0, f"cpp_mux_consumer failed: {result.stderr}"
        assert f"CONSUMED {n}" in result.stdout
        assert f"PRODUCER 0 {n}" in result.stdout
        assert "PRODUCER 1" not in result.stdout
        print(f"  C++ consumer received exactly {n} messages from the registered producer")
    finally:
        mux.unlink()
        mux.close()


class NoReadLast(QueueTraits):
    """enable_read_last is the one trait that reaches shared memory: the queue's
    creator writes 'SLQ0' instead of 'SLQ1' and every peer must agree. C++ twin:
    interop_no_read_last_traits in interop_common.hpp (--no-read-last)."""
    enable_read_last = False


def _ready(proc: subprocess.Popen) -> None:
    """Wait for a C++ producer's READY line, surfacing its error if it died."""
    line = proc.stdout.readline()
    assert "READY" in line, \
        f"unexpected producer output: {line!r} {'' if line else proc.stderr.read()}"


def test_read_last_off_python_queue():
    """Python creates the queue with enable_read_last=False ('SLQ0'). A C++
    consumer with the default traits is rejected at attach time; a C++ producer
    and consumer built with matching traits interoperate with Python."""
    print("TEST: enable_read_last=False queue created by Python")
    n = 50  # per producer
    mux = StreamBufferMultiplexer(shared_queue_size=1024, name="mux_io_nrlp_records",
                                  traits=NoReadLast)
    cpp_producer = None
    try:
        p0 = mux.add_producer(0, capacity=1 << 16, control_size=256, name="mux_io_nrlp_p0")

        result = subprocess.run(
            [find_cpp_executable("cpp_mux_consumer"), mux.get_shm_name(), "1",
             "--producer", "0", p0.get_shm_name()],
            capture_output=True, text=True, timeout=60)
        assert result.returncode != 0, "default-traits C++ consumer attached to an 'SLQ0' queue"
        assert "Shared memory feature mismatch" in result.stderr, result.stderr
        print("  default-traits C++ consumer rejected: feature mismatch")

        cpp_producer = subprocess.Popen(
            [find_cpp_executable("cpp_mux_producer"), mux.get_shm_name(), "mux_io_nrlp_p1",
             "1", str(n), "create-producer", str(1 << 16), "256", "--hold", "--no-read-last"],
            stdin=subprocess.PIPE, stdout=subprocess.PIPE, stderr=subprocess.PIPE, text=True)
        _ready(cpp_producer)
        mux.add_producer(1, name="mux_io_nrlp_p1")

        for i in range(n):
            publish_msg(p0, i)

        python_read_all(mux, {0: n, 1: n})
        print(f"  Python consumer saw all {2 * n} messages from both languages")

        result = subprocess.run(
            [find_cpp_executable("cpp_mux_consumer"), mux.get_shm_name(), str(2 * n),
             "--producer", "0", p0.get_shm_name(), "--producer", "1", "mux_io_nrlp_p1",
             "--no-read-last"],
            capture_output=True, text=True, timeout=60)
        assert result.returncode == 0, f"cpp_mux_consumer failed: {result.stderr}"
        assert f"CONSUMED {2 * n}" in result.stdout
        print(f"  matching C++ consumer saw all {2 * n} messages from both languages")

        line = cpp_producer.stdout.readline()
        assert f"PUBLISHED {n}" in line, f"unexpected producer output: {line!r}"
    finally:
        if cpp_producer is not None:
            _finish(cpp_producer)
        mux.close()
        mux.unlink()


def test_read_last_off_cpp_queue():
    """C++ creates the queue with enable_read_last=False ('SLQ0') and publishes.
    A Python opener with the default traits is rejected; one with matching
    traits consumes every message."""
    print("TEST: enable_read_last=False queue created by C++")
    n = 50
    queue_name, producer_name = "mux_io_nrlc_records", "mux_io_nrlc_p0"
    cpp_producer = subprocess.Popen(
        [find_cpp_executable("cpp_mux_producer"), queue_name, producer_name, "0", str(n),
         "create-queue", "1024", "create-producer", str(1 << 16), "256",
         "--hold", "--no-read-last"],
        stdin=subprocess.PIPE, stdout=subprocess.PIPE, stderr=subprocess.PIPE, text=True)
    mux = None
    try:
        _ready(cpp_producer)

        try:
            StreamBufferMultiplexer(name=queue_name)
            assert False, "default-traits Python opener attached to an 'SLQ0' queue"
        except RuntimeError as e:
            assert "Shared memory feature mismatch" in str(e), str(e)
        print("  default-traits Python opener rejected: feature mismatch")

        mux = StreamBufferMultiplexer(name=queue_name, traits=NoReadLast)
        assert not mux.traits.enable_read_last
        mux.add_producer(0, name=producer_name)
        python_read_all(mux, {0: n})
        print(f"  matching Python consumer saw all {n} messages from the C++ producer")

        line = cpp_producer.stdout.readline()
        assert f"PUBLISHED {n}" in line, f"unexpected producer output: {line!r}"
    finally:
        _finish(cpp_producer)
        if mux is not None:
            mux.close()
            mux.unlink()


def test_python_queue_producer_cpp_consumer():
    """Python creates the queue, a shared-memory queue producer (items_per_slot
    16, so the segment carries the 'SLQ3' marker) and a stream buffer producer,
    and publishes through both; a C++ consumer registers each as its kind and
    verifies global order and content."""
    print("TEST: Python queue + stream buffer producers -> C++ consumer")
    n = 50  # per producer
    mux = StreamBufferMultiplexer(shared_queue_size=1024, name="mux_io_qp2c_records")
    try:
        q0 = mux.add_queue_producer(0, size=1 << 16, items_per_slot=16, name="mux_io_qp2c_q0")
        p1 = mux.add_producer(1, capacity=1 << 16, control_size=256, name="mux_io_qp2c_p1")

        consumer = subprocess.Popen(
            [find_cpp_executable("cpp_mux_consumer"), mux.get_shm_name(), str(2 * n),
             "--queue-producer", "0", q0.get_shm_name(),
             "--producer", "1", p1.get_shm_name()],
            stdout=subprocess.PIPE, stderr=subprocess.PIPE, text=True)

        for i in range(n):  # interleave the two kinds
            publish_msg(q0, i)
            publish_msg(p1, i)

        out, err = consumer.communicate(timeout=60)
        assert consumer.returncode == 0, f"cpp_mux_consumer failed (rc={consumer.returncode}): {err}"
        assert f"CONSUMED {2 * n}" in out, f"unexpected output: {out}"
        assert f"PRODUCER 0 {n}" in out and f"PRODUCER 1 {n}" in out
        print(f"  C++ consumed all {2 * n} messages from a Python queue and stream buffer producer")
    finally:
        mux.unlink()
        mux.close()


def test_cpp_queue_producer_python_consumer():
    """Python creates the record queue; a C++ process creates a queue producer
    segment (slick::queue<uint8_t> with items_per_slot 16) and publishes while a
    Python stream buffer producer publishes alongside; Python opens the C++
    queue producer by name - adopting its size and items_per_slot - and
    consumes both."""
    print("TEST: C++ queue producer + Python stream buffer producer -> Python consumer")
    n = 50  # per producer
    mux = StreamBufferMultiplexer(shared_queue_size=1024, name="mux_io_qc2p_records")
    cpp_producer = None
    try:
        p1 = mux.add_producer(1, capacity=1 << 16, control_size=256, name="mux_io_qc2p_p1")

        cpp_producer = subprocess.Popen(
            [find_cpp_executable("cpp_mux_producer"), mux.get_shm_name(), "mux_io_qc2p_q0",
             "0", str(n), "create-queue-producer", str(1 << 16), "16", "--hold"],
            stdin=subprocess.PIPE, stdout=subprocess.PIPE, stderr=subprocess.PIPE, text=True)
        _ready(cpp_producer)

        q0 = mux.add_queue_producer(0, name="mux_io_qc2p_q0")
        assert q0.size == 1 << 16
        assert q0.items_per_slot == 16
        assert not q0.own_buffer

        for i in range(n):
            publish_msg(p1, i)

        python_read_all(mux, {0: n, 1: n})
        print(f"  Python consumer saw all {2 * n} messages from both languages and kinds")

        line = cpp_producer.stdout.readline()
        assert f"PUBLISHED {n}" in line, f"unexpected producer output: {line!r}"
    finally:
        if cpp_producer is not None:
            _finish(cpp_producer)
        mux.unlink()
        mux.close()


ALL_TESTS = {
    "python_producers_cpp_consumer": test_python_producers_cpp_consumer,
    "cpp_producers_python_consumer": test_cpp_producers_python_consumer,
    "mixed_language_producers": test_mixed_language_producers,
    "partial_registration_interop": test_partial_registration_interop,
    "read_last_off_python_queue": test_read_last_off_python_queue,
    "read_last_off_cpp_queue": test_read_last_off_cpp_queue,
    "python_queue_producer_cpp_consumer": test_python_queue_producer_cpp_consumer,
    "cpp_queue_producer_python_consumer": test_cpp_queue_producer_python_consumer,
}


def main():
    parser = argparse.ArgumentParser(description="C++/Python multiplexer interop tests")
    parser.add_argument("--test", choices=sorted(ALL_TESTS), default=None,
                        help="run a single test (default: all)")
    args = parser.parse_args()

    tests = {args.test: ALL_TESTS[args.test]} if args.test else ALL_TESTS

    print("=" * 70)
    print("Running C++/Python Multiplexer Interop Tests")
    print("=" * 70)

    passed = 0
    failed = 0
    for name, func in tests.items():
        try:
            print(f"\n[TEST] {name}...")
            func()
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
    success = main()
    sys.exit(0 if success else 1)
