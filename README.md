# slick-stream-buffer-multiplexer-py

A Python implementation of [slick-stream-buffer-multiplexer](https://github.com/SlickQuant/slick-stream-buffer-multiplexer) —
a lock-free multi-producer multi-consumer (MPMC) byte stream multiplexer with shared memory support.

**Maintains exact binary compatibility with the C++ version**: Python and C++ producers and
consumers can be mixed across processes, fanning into the same shared memory segments — provided
every peer of a shared queue agrees on `enable_read_last` (see
[`enable_read_last` must match on every peer](#enable_read_last-must-match-on-every-peer)).

Tracks C++ `slick/stream_buffer_multiplexer.hpp` **v2.2.0**, and requires slick-queue-py >=
2.1.0 and slick-stream-buffer-py >= 2.0.0. The shared-memory formats are unchanged
(`'SLQ1'` / `'SSB1'`), so segments written by v0.1.0 or by a C++ v1.x process are still
readable with the default traits.

## Concept

Each producer owns its own byte ring, of one of two kinds, freely mixed under one multiplexer and
one `producer_id` space:

- a [slick-stream-buffer](https://github.com/SlickQuant/slick-stream-buffer) (an
  independently-sized byte ring with per-producer lap/loss detection), written with
  `prepare`/`commit`/`consume` from one thread;
- a byte [slick-queue](https://github.com/SlickQuant/slick-queue) (`element_size=1`, the C++
  `slick::queue<uint8_t>`), written with `reserve`/`publish`, which several threads may do at once.

Publishing through either additionally publishes a small 16-byte `{sequence, producer_id}` record
into one shared slick-queue, which acts as the lock-free MPMC fan-in / global ordering point.
Consumers read from the shared queue and dereference each record back into the matching
producer's ring, getting `(data, length, producer_id)` whichever kind published it.

```
Producer A ──prepare/commit/consume──▶ [StreamBuffer A] ──┐
Producer B ──prepare/commit/consume──▶ [StreamBuffer B] ──┼─▶ {sequence, producer_id}
Producer C ──reserve/write/publish───▶ [byte SlickQueue C]┘        16-byte records
                                                                         │
                                                              [shared SlickQueue]
                                                                         │
                                          consumers read(cursor) and dereference
                                          into the ring of producer_id
```

Producers and the shared queue each independently choose local memory or shared memory (IPC).
A cross-process consumer only registers the producer ids whose shared memory it can access;
records referencing other producer ids are silently skipped (not counted as loss).

## Requirements

- Python 3.8+
- [slick-queue-py](https://github.com/SlickQuant/slick-queue-py) >= 2.1.0 (queue producers need
  its `items_per_slot`)
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

### Queue producers

A producer can be a byte slick-queue instead. Register it with `add_queue_producer()` and publish
through the `ProducerQueue` it returns, which has slick-queue's own producer interface:

```python
# 1 MB byte queue, minimum 64 bytes per reservation (both must be powers of 2)
fills = mux.add_queue_producer(2, size=1 << 20, items_per_slot=64, name="fills")

index = fills.reserve(len(message))   # len(message) contiguous bytes; never blocks
fills[index][:len(message)] = message # fills[index] is a writable view from index onward
fills.publish(index, len(message))    # publish as ONE message + fan into the shared queue
```

One message is the bytes of one `reserve()`/`publish()` pair, and consumers get it exactly as they
get a stream buffer message. What differs is the producer side:

- **Several threads may publish through one `ProducerQueue`.** slick-queue is MPMC, and so is the
  fan-in. Each thread's own messages stay in order.
- **Give a large queue an `items_per_slot`.** It is the minimum number of bytes one `reserve()`
  consumes, and slick-queue keeps one 16-byte control slot per unit of it. Without it there is a
  slot per byte - 16 MB of control for 1 MB of data. See
  [slick-queue-py](https://github.com/SlickQuant/slick-queue-py#byte-buffers-and-items_per_slot).
- **A message is limited to the queue's size.** `reserve()` raises `RuntimeError` for a larger
  one and `ValueError` for 0, as `SlickQueue.reserve()` does.

The forms mirror the C++ overloads: `add_queue_producer(id, size=...[, items_per_slot=...])` for
local memory, add `name=...` to create a segment, `add_queue_producer(id, name=...)` to open one
(its size and `items_per_slot` come from the segment), or `add_queue_producer(id, queue=q)` to
adopt a byte `SlickQueue` you already have. Only what is published through the handle is
multiplexed: a message put on an adopted queue directly reaches whoever reads that queue, but
`mux.read()` never returns it.

`producer_id` is one namespace across both kinds: an id registered with `add_producer()` cannot be
registered again with `add_queue_producer()`, or the other way round. `has_producer()` and
`producer_count()` cover both; `get_producer_queue()`, `find_producer_queue()` and
`get_producer_queues()` are the queue counterparts of the stream buffer accessors, and each kind's
accessors return `None` for the other kind's ids.

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
shared queue. Stream buffer producer segments (`'SSB1'`) carry no such flag. Queue producers are
built with the multiplexer's traits, as in C++, so their segments carry the same marker and the
same rule applies to them.

## Three Loss Counters

| Counter | Meaning |
|---|---|
| `mux.loss_count()` | shared-queue wrap loss **plus** multiplexer-level loss (registered producers lapped before dereference); both terms switched by `traits.enable_loss_detection` |
| `ProducerBuffer.loss_count()` | that producer's own inner-ring loss — **only** for reads the caller makes through `producer.stream_buffer.read(cursor, traits)` with counting traits |
| `ProducerQueue.loss_count()` | the inner queue's own counter — a diagnostic of that queue, **not** a record count (see below) |
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

`ProducerQueue.loss_count()` cannot be kept clean the same way: slick-queue configures loss
detection per queue, not per call, so with counting traits (slick-queue-py's default) every
dereference that finds its record lapped adds slick-queue's "how far has this slot moved on"
distance to it - and each later lapped dereference adds its own again. Ten one-byte messages
through a 4-byte queue lose 6 records, which `mux.loss_count()` reports as 6 while that queue's
own counter reads 32. Read loss off the multiplexer.

**A queue message is lost once its bytes may be overwritten, not only its slot.** Messages differ
in length, so a later reservation can cover an older message's bytes without landing on its
control slot: in an 8-byte queue, `"ijkl"` at index 8 overwrites `"AB"` at 0 and `"CD"` at 2 while
`"CD"`'s slot still names it. `read()` therefore also skips (and counts) any queue message the
reservation counter has moved more than one ring past. `read()` copies the bytes before it checks,
so the check covers the copy you get back. It is conservative, as in C++: a reservation that does
not fit before the end of the ring skips the rest of the lap, so the counter also passes indices
nothing wrote, and a message lying in that skipped tail is reported lost although its bytes are
intact.

`mux.get_producer_buffers()` returns every registered stream buffer producer keyed by
`producer_id`, and `mux.get_producer_queues()` every queue producer — read-only live views of the
registration tables, cheap enough for a monitoring loop:

```python
inner_loss = sum(p.loss_count() for p in mux.get_producer_buffers().values())
queues = {pid: p.size for pid, p in mux.get_producer_queues().items()}
```

## Shutdown

Close every handle, then unlink once — the usual `shared_memory` idiom:

```python
mux.close()      # or leave a `with StreamBufferMultiplexer(...) as mux:` block
mux.unlink()     # deletes the record queue and every registered producer's segment, both kinds
```

`close()` keeps the registration table so `unlink()` can still reach each producer's segment, and
`unlink()` runs once: repeating it never deletes a segment a later run has created under the same
name. On Windows `unlink()` is a no-op — a section disappears once its last handle closes.

## Stale-segment recovery

`StreamBufferMultiplexer.remove(name)` unlinks a segment by name — it works for every kind this
class creates, a shared record queue's, a producer stream buffer's and a producer queue's, since
all are plain shared memory segments underneath. Use it when a previous run died mid-initialization and left a
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
auto fills = mux.add_queue_producer(2, "fills");       // and its queue producer's

uint64_t cursor = mux.initial_reading_index();
for (;;) {
    auto rec = mux.read(cursor);
    if (!rec) continue;
    handle_message(rec.producer_id, rec.data, rec.length);
}
```

Use `get_shm_name()` on the multiplexer (queue segment) and each producer to get the exact names
to pass to C++ (POSIX names include the required leading `/`). Each side must register an id as
the kind it is: the record carries only `{sequence, producer_id}`, so the kind behind an id is part
of the same convention as its name. A consumer on C++ 2.1 or earlier, or on this package before
2.2.0, has no queue producers; it skips their records as unregistered, without counting them as
loss.

### Binary layout

The multiplexer adds no shared-memory structures of its own:

- The shared queue is a standard slick-queue segment (`'SLQ1'`, or `'SLQ0'` when created with
  `enable_read_last` off; `element_size=16`) whose elements are
  `uint64 sequence | uint32 producer_id | uint32 pad0`; `sequence` is a stream buffer's record
  sequence or a queue producer's reservation index
- Each stream buffer producer is a standard slick-stream-buffer segment (`'SSB1'`)
- Each queue producer is a standard slick-queue segment with `element_size=1`, marked like any
  slick-queue segment: `'SLQ1'` by default, with the `items_per_slot` bit added (`'SLQ3'`) when
  `items_per_slot` is not 1, and the read-last bit cleared (`'SLQ0'` / `'SLQ2'`) when created
  with `enable_read_last` off

## Caveats (same as C++)

- `add_producer()` and `add_queue_producer()` are single-threaded setup: call them before
  producer/consumer threads start.
- A `producer_id` must fit the record's `uint32` field: both registration calls raise
  `ValueError` for one outside `0..0xFFFFFFFF` (and `TypeError` for a non-integer), before any
  segment is created.
- Each `ProducerBuffer`'s `prepare/commit/consume/...` must be called from a single thread. A
  `ProducerQueue` has no such limit.
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
python tests/test_multiplexer_queue.py
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
