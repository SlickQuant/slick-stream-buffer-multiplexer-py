# slick-stream-buffer-multiplexer-py

A Python implementation of [slick-stream-buffer-multiplexer](https://github.com/SlickQuant/slick-stream-buffer-multiplexer) —
a lock-free multi-producer multi-consumer (MPMC) byte stream multiplexer with shared memory support.

**Maintains exact binary compatibility with the C++ version**: Python and C++ producers and
consumers can be mixed freely across processes, fanning into the same shared memory segments.

## Concept

Each producer owns its own [slick-stream-buffer](https://github.com/SlickQuant/slick-stream-buffer)
(an independently-sized byte ring with per-producer lap/loss detection). `consume()` additionally
publishes a small 16-byte `{sequence, producer_id}` record into one shared
[slick-queue](https://github.com/SlickQuant/slick-queue), which acts as the lock-free MPMC
fan-in / global ordering point. Consumers read from the shared queue and dereference each record
back into the matching producer's buffer.

```
Producer A ──prepare/commit/consume──▶ [StreamBuffer A] ──┐
Producer B ──prepare/commit/consume──▶ [StreamBuffer B] ──┼─▶ {sequence, producer_id}
Producer N ──prepare/commit/consume──▶ [StreamBuffer N] ──┘        16-byte records
                                                                         │
                                                              [shared SlickQueue]
                                                                         │
                                          consumers read(cursor) and dereference
                                          into StreamBuffer[producer_id]
```

Producers and the shared queue each independently choose local memory or shared memory (IPC).
A cross-process consumer only registers the producer ids whose shared memory it can access;
records referencing other producer ids are silently skipped (not counted as loss).

## Requirements

- Python 3.8+
- [slick-queue-py](https://github.com/SlickQuant/slick-queue-py) >= 1.2.0
- [slick-stream-buffer-py](https://github.com/SlickQuant/slick-stream-buffer-py) >= 0.1.0

Both dependencies bundle a small C++ extension for `std::atomic` operations; this package itself
is pure Python.

## Installation

```bash
pip install slick-queue-py slick-stream-buffer-py
pip install -e .
```

## Quick Start

### Producers (any process, any language)

```python
from slick_stream_buffer_multiplexer_py import StreamBufferMultiplexer

# create the shared record queue (the fan-in ordering point)
mux = StreamBufferMultiplexer(shared_queue_size=1 << 16, name="mux_records")

# each producer gets its own independently-sized stream buffer
feed_a = mux.add_producer(0, capacity=1 << 26, control_size=1 << 16, name="feed_a")
feed_b = mux.add_producer(1, capacity=1 << 20, control_size=1 << 10, name="feed_b")

mv = feed_a.prepare(64 * 1024)     # contiguous writable memoryview (zero-copy)
n = sock.recv_into(mv)             # write network bytes directly into the ring
feed_a.commit(n)
feed_a.consume(n)                  # publish as ONE message + fan into the shared queue
```

### Consumer (opens everything created elsewhere — Python or C++)

```python
mux = StreamBufferMultiplexer(name="mux_records")     # open the shared record queue
mux.add_producer(0, name="feed_a")                    # register the producers to follow
mux.add_producer(1, name="feed_b")                    # (geometry read from each header)

cursor = mux.initial_reading_index()                  # or 0 to read history
while True:
    rec, cursor = mux.read(cursor)
    if not rec:
        continue
    handle_message(rec.producer_id, rec.data, rec.length)
```

### Work-stealing consumers (each message to exactly one consumer)

```python
from slick_stream_buffer_multiplexer_py import AtomicCursor
from multiprocessing.shared_memory import SharedMemory

cursor_shm = SharedMemory(name="mux_cursor", create=True, size=8)
shared_cursor = AtomicCursor(cursor_shm.buf, 0)

rec, idx = mux.read(shared_cursor)   # atomically claims the next message
```

## Three Loss Counters

| Counter | Meaning |
|---|---|
| `mux.loss_count()` | shared-queue wrap loss **plus** multiplexer-level loss (registered producers lapped before dereference) |
| `producer.loss_count()` | that producer's own inner-ring loss |
| unregistered producer ids | **never counted** — silently skipped |

## C++ Interoperability

The C++ side composes the identical primitives — either language can create any segment:

```cpp
#include <slick/stream_buffer_multiplexer.hpp>

slick::stream_buffer_multiplexer mux("mux_records");   // open queue created by Python
auto feed = mux.add_producer(0, "feed_a");             // open Python's producer segment

uint64_t cursor = mux.initial_reading_index();
for (;;) {
    auto rec = mux.read(cursor);
    if (!rec) continue;
    handle_message(rec.producer_id, rec.data, rec.length);
}
```

Use `get_shm_name()` on the multiplexer (queue segment) and each producer buffer to get the
exact names to pass to C++ (POSIX names include the required leading `/`).

### Binary layout

The multiplexer adds no shared-memory structures of its own:

- The shared queue is a standard slick-queue segment (`'SLQ1'`, `element_size=16`) whose
  elements are `uint64 sequence | uint32 producer_id | uint32 pad0`
- Each producer buffer is a standard slick-stream-buffer segment (`'SSB1'`)

## Caveats (same as C++)

- `add_producer()` is single-threaded setup: call it before producer/consumer threads start.
- Each producer's `prepare/commit/consume/...` must be called from a single thread.
- Lossy semantics: slow consumers skip overwritten data (see the loss counters above).
- A single message (one `consume()` call) is limited to < 4 GiB.

See [API_DIFFERENCES.md](API_DIFFERENCES.md) for exact deviations from the C++ API.

## Building and Testing

```bash
# Pure-Python tests (sibling repos slick-queue-py / slick-stream-buffer-py are
# picked up automatically from ../ if not pip-installed)
python tests/test_multiplexer.py
python tests/test_multiplexer_mpmc.py
python tests/test_multiplexer_shm.py

# C++ interop tests (requires CMake + a C++20 compiler)
cmake -S . -B build
cmake --build build --config Debug
cd build && ctest -C Debug --output-on-failure
```

The interop tests fetch slick-stream-buffer-multiplexer (which pulls slick-stream-buffer,
slick-queue, and slick-shm) from GitHub and build real C++ producer/consumer binaries against
the actual header. To build against local checkouts instead (no network):

```bash
cmake -S . -B build \
  -DFETCHCONTENT_SOURCE_DIR_SLICK-STREAM-BUFFER-MULTIPLEXER=/path/to/slick-stream-buffer-multiplexer \
  -DFETCHCONTENT_SOURCE_DIR_SLICK-STREAM-BUFFER=/path/to/slick-stream-buffer \
  -DFETCHCONTENT_SOURCE_DIR_SLICK-QUEUE=/path/to/slick-queue \
  -DFETCHCONTENT_SOURCE_DIR_SLICK-SHM=/path/to/slick-shm
```

If a failed test run leaves segments behind: `python tests/cleanup_shm.py`

## Related Projects

- [slick-stream-buffer-multiplexer](https://github.com/SlickQuant/slick-stream-buffer-multiplexer) — the C++ implementation
- [slick-stream-buffer-py](https://github.com/SlickQuant/slick-stream-buffer-py) / [slick-stream-buffer](https://github.com/SlickQuant/slick-stream-buffer) — the per-producer SPMC byte stream buffer
- [slick-queue-py](https://github.com/SlickQuant/slick-queue-py) / [slick-queue](https://github.com/SlickQuant/slick-queue) — the shared MPMC record queue

## License

MIT
