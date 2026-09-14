# slick-stream-buffer-multiplexer-py

A Python implementation of [slick-stream-buffer-multiplexer](https://github.com/SlickQuant/slick-stream-buffer-multiplexer) —
a lock-free multi-producer multi-consumer (MPMC) byte stream multiplexer with shared memory support.

**Maintains exact binary compatibility with the C++ version**: Python and C++ producers and
consumers can be mixed across processes, fanning into the same shared memory segments — provided
every peer of a shared queue agrees on `enable_read_last` (see
[`enable_read_last` must match on every peer](#enable_read_last-must-match-on-every-peer)).

Tracks C++ `slick/stream_buffer_multiplexer.hpp` **v2.0.0**, and requires slick-queue-py >=
2.0.0 and slick-stream-buffer-py >= 2.0.0. The shared-memory formats are unchanged
(`'SLQ1'` / `'SSB1'`), so segments written by v0.1.0 or by a C++ v1.x process are still
readable with the default traits.

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
- [slick-queue-py](https://github.com/SlickQuant/slick-queue-py) >= 2.0.0
- [slick-stream-buffer-py](https://github.com/SlickQuant/slick-stream-buffer-py) >= 2.0.0

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

## Traits

Feature configuration is a `traits` constructor argument — and it is slick-queue-py's
`QueueTraits`, the traits of the shared record queue the multiplexer is built on, not a traits
type of its own:

```python
from slick_stream_buffer_multiplexer_py import StreamBufferMultiplexer, QueueTraits

class Silent(QueueTraits):
    enable_loss_detection = False       # both loss terms go quiet together

mux = StreamBufferMultiplexer(shared_queue_size=1024, traits=Silent)
```

Every knob the multiplexer has beyond that queue is a passthrough, and its one real tunable
asks the same question `enable_loss_detection` already asks — so a separate flag would only let
the two disagree. That matters because `loss_count()` **sums** the two counters: a
half-configured pair would return a partial total that reads like a complete one. One flag, one
number. `mux.traits` is the queue's own frozen snapshot, not a second copy.

Unlike C++ — where `default_queue_traits` follows `NDEBUG` — slick-queue-py defaults
`enable_loss_detection` to **on**, so the counters work out of the box.

### `enable_read_last` must match on every peer

The other traits are local to one instance and may differ freely between processes, but
`enable_read_last` is part of the shared record queue's header protocol. The queue's creator
stamps it into the segment's layout marker — `'SLQ1'` when the last-published index is maintained,
`'SLQ0'` when it is not — and every attacher, Python or C++, checks the marker against its own
traits. A peer that disagrees is refused at construction (`RuntimeError` in Python,
`std::runtime_error` in C++, both reading *"Shared memory feature mismatch"*) rather than
misreading the header: a Python queue created with `enable_read_last = False` cannot be opened by a
C++ `slick::stream_buffer_multiplexer`, whose default traits have it on.

Both languages default it to on, so only turning it off needs coordinating — do it identically on
every peer of the queue:

```python
class NoReadLast(QueueTraits):
    enable_read_last = False

mux = StreamBufferMultiplexer(shared_queue_size=1 << 16, name="mux_records", traits=NoReadLast)
```

```cpp
struct no_read_last_traits : slick::default_queue_traits {
    static constexpr bool enable_read_last = false;
};
slick::basic_stream_buffer_multiplexer<no_read_last_traits> mux("mux_records");
```

The multiplexer itself never calls `read_last()` (`initial_reading_index()` reads the reservation
counter instead), so turning it off only drops one CAS from each `consume()`'s publish into the
shared queue. Producer segments (`'SSB1'`) carry no such flag.

## Three Loss Counters

| Counter | Meaning |
|---|---|
| `mux.loss_count()` | shared-queue wrap loss **plus** multiplexer-level loss (registered producers lapped before dereference); both terms switched by `traits.enable_loss_detection` |
| `producer.loss_count()` | that producer's own inner-ring loss — **only** for reads the caller makes through `producer.stream_buffer.read(cursor, traits)` with counting traits |
| unregistered producer ids | **never counted** — silently skipped |

Only the multiplexer-level term is filtered by registration. The wrap-loss term counts entries
the shared queue dropped by wrapping, and a lapped slot has already been overwritten, so nothing
is left to say which producer it named — an instance registering a subset of producers therefore
sees wrap loss for producers it ignores, making the total an upper bound on what it missed. Size
the shared queue so it does not wrap and that term goes to 0.

`producer.loss_count()` stays at 0 under multiplexer traffic by design: the multiplexer
dereferences through traits with `count_loss` off, because `read()`'s counter measures how far
a *sequential scan* jumped, which for a jump-read is a distance every later lapped dereference
would add all over again. The multiplexer counts one loss per lapped record instead.

`mux.get_producer_buffers()` returns every registered producer keyed by `producer_id` — a
read-only live view of the registration table, cheap enough for a monitoring loop:

```python
inner_loss = sum(p.loss_count() for p in mux.get_producer_buffers().values())
```

## Shutdown

Close every handle, then unlink once — the usual `shared_memory` idiom:

```python
mux.close()      # or leave a `with StreamBufferMultiplexer(...) as mux:` block
mux.unlink()     # deletes the record queue and every registered producer's segment
```

`close()` keeps the registration table so `unlink()` can still reach each producer's segment, and
`unlink()` runs once: repeating it never deletes a segment a later run has created under the same
name. On Windows `unlink()` is a no-op — a section disappears once its last handle closes.

## Stale-segment recovery

`StreamBufferMultiplexer.remove(name)` unlinks a segment by name — it works for both kinds this
class creates, a shared record queue's and a producer stream buffer's, since both are plain
shared memory segments underneath. Use it when a previous run died mid-initialization and left a
segment wedged, which construction over that name reports by raising:

```python
StreamBufferMultiplexer.remove("md_records")        # clear what a dead run left
mux = StreamBufferMultiplexer(shared_queue_size=1024, name="md_records")
```

Only call it when no process is using the segment. On Windows it is a no-op returning `True` —
a section there disappears once the last handle closes.

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

- The shared queue is a standard slick-queue segment (`'SLQ1'`, or `'SLQ0'` when created with
  `enable_read_last` off; `element_size=16`) whose elements are
  `uint64 sequence | uint32 producer_id | uint32 pad0`
- Each producer buffer is a standard slick-stream-buffer segment (`'SSB1'`)

## Caveats (same as C++)

- `add_producer()` is single-threaded setup: call it before producer/consumer threads start.
- A `producer_id` must fit the record's `uint32` field: `add_producer()` raises `ValueError`
  for one outside `0..0xFFFFFFFF` (and `TypeError` for a non-integer), before any segment is
  created.
- Each producer's `prepare/commit/consume/...` must be called from a single thread.
- Lossy semantics: slow consumers skip overwritten data (see the loss counters above).
  Records are skipped correctly whatever the traits say; only the counters go quiet.
- A single message (one `consume()` call) is limited to < 4 GiB: `consume()` raises
  `ValueError` rather than truncating one that is not, before any state moves — so nothing is
  published to either the producer's ring or the shared record queue.

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
