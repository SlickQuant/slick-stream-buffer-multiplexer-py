# API Differences: Python vs C++

The Python implementation is binary-compatible with the C++
`slick::stream_buffer_multiplexer` at the shared memory level — the shared record queue
('SLQ1', or 'SLQ0' with `enable_read_last` off; 16-byte elements) and every producer segment
('SSB1') match byte-for-byte. Peers must agree on `enable_read_last`, the one trait recorded in
the queue's header (see [Traits](#traits-are-a-constructor-argument-not-a-template-parameter)).
The *language-level* API differs in the following ways.

## Construction

| C++ | Python |
|---|---|
| `stream_buffer_multiplexer mux(size)` | `StreamBufferMultiplexer(shared_queue_size=size)` |
| `stream_buffer_multiplexer mux(size, "name")` | `StreamBufferMultiplexer(shared_queue_size=size, name="name")` |
| `stream_buffer_multiplexer mux("name")` | `StreamBufferMultiplexer(name="name")` |
| `add_producer(id, cap, ctl)` | `add_producer(id, capacity=cap, control_size=ctl)` |
| `add_producer(id, cap, ctl, "name")` | `add_producer(id, capacity=cap, control_size=ctl, name="name")` |
| `add_producer(id, "name")` | `add_producer(id, name="name")` |
| throws `std::invalid_argument` (duplicate id, bad sizes) | raises `ValueError` |
| `uint32_t producer_id` (range enforced by the type) | raises `ValueError` outside `0..0xFFFFFFFF`, `TypeError` for a non-integer |
| throws `std::runtime_error` (shm failures) | raises `RuntimeError`, `ValueError` (queue geometry mismatch), or `FileNotFoundError` (missing segment) |

## Consumer cursor is passed by value

```cpp
// C++
uint64_t cursor = 0;
auto rec = mux.read(cursor);              // cursor updated in place
```

```python
# Python
cursor = 0
rec, cursor = mux.read(cursor)            # updated cursor returned
```

For the work-stealing overload, pass an `AtomicCursor` (from slick-queue-py); the second
return value is then the claimed shared-queue index (`-1` when nothing was available):

```python
rec, idx = mux.read(shared_cursor)
```

## Zero-copy vs copies

Same policy as slick-stream-buffer-py: `prepare()` and `data()` return zero-copy
memoryviews into the producer's ring; `read()` results (`MultiplexRecord.data`) and
`consume()` results are **bytes copies**, safe by default under the lossy overwrite
semantics.

## Producer buffer accessors

C++ exposes both `stream_buffer()` (reference) and `stream_buffer_ptr()`
(`std::shared_ptr`); Python has no owning/non-owning distinction, so there is a single
`producer.stream_buffer` property returning the underlying `SlickStreamBuffer`.
Similarly, `get_producer_buffer()` and `find_producer()` both return the same
`ProducerBuffer` object (or `None`).

## Producer lookup

C++ uses a dense vector for producer ids < 4096 with a hash-map fallback; Python uses a
plain dict for all ids (already O(1) — the dense-path micro-optimization is meaningless
in Python).

`get_producer_buffers()` returns a `const` map reference in C++; Python returns a cached
read-only `types.MappingProxyType` over the same dict — mutation raises, later
`add_producer()` calls still show through, and the call allocates nothing. Iteration order
is insertion order, which C++'s `unordered_map` does not promise.

## Traits are a constructor argument, not a template parameter

C++ spells the configuration `basic_stream_buffer_multiplexer<Traits = default_queue_traits>`;
Python passes the same `QueueTraits` class as a `traits=` keyword argument, matching how
slick-queue-py does it:

```cpp
// C++
slick::basic_stream_buffer_multiplexer<slick::debug_queue_traits> mux(1024);
```

```python
# Python
mux = StreamBufferMultiplexer(shared_queue_size=1024, traits=MyTraits)
```

`mux.traits` returns the shared queue's frozen snapshot (C++ spells this
`using traits = Traits`). One behavioural difference, inherited from slick-queue-py:
C++'s `default_queue_traits` follows `NDEBUG`, so loss counting is off in release builds;
Python has no debug/release split and defaults `enable_loss_detection` to on.

That difference is harmless across processes because only one trait reaches shared memory:
`enable_read_last`. The queue's creator records it in the layout marker (`'SLQ1'` on, `'SLQ0'`
off) and every attacher in either language must be configured the same way, or construction fails
with "Shared memory feature mismatch" (`RuntimeError` / `std::runtime_error`). Both sides default
it to on; a peer that turns it off needs its counterparts to do the same:

```cpp
// C++
struct no_read_last_traits : slick::default_queue_traits {
    static constexpr bool enable_read_last = false;
};
slick::basic_stream_buffer_multiplexer<no_read_last_traits> mux("mux_records");
```

```python
# Python
class NoReadLast(QueueTraits):
    enable_read_last = False

mux = StreamBufferMultiplexer(name="mux_records", traits=NoReadLast)
```

## loss_count()

Same semantics on both sides: `traits.enable_loss_detection` switches the shared queue's
wrap-loss term and the multiplexer-level term together, and both read 0 when it is off. Only
the multiplexer-level term is filtered by producer registration. Records are skipped correctly
either way. All counters are per-instance, not shared through the segments.

`ProducerBuffer.loss_count()` is likewise identical: the multiplexer dereferences through
traits with `count_loss` off, so that counter only moves for reads the caller makes directly
through `producer.stream_buffer.read(cursor, traits)`.

## producer_buffer is not movable in C++

C++ deletes `producer_buffer`'s copy and move operations, because the multiplexer keeps a raw
pointer to the same object its `shared_ptr` names, and moving one out would leave both pointing
at an emptied object. Python has no moves and rebinding a name copies a reference, so there is
nothing to delete — `ProducerBuffer` has no equivalent restriction.

## Lifecycle

Python adds explicit lifecycle management that C++ handles via RAII:

- `close()` — detach from all producer segments and the queue segment; the registration table is
  kept, so a later `unlink()` still reaches every producer
- `unlink()` — delete the queue segment and all registered producers' segments; valid before or
  after `close()` (close every handle, then unlink once), and a no-op when repeated
- context manager support: `with StreamBufferMultiplexer(...) as mux: ...`
- `get_shm_name()` — on the multiplexer (queue segment) and on each `ProducerBuffer`,
  the exact names to pass to C++ (`/`-prefixed on POSIX)

`remove(name)` is the one lifecycle call both sides share, with the same contract: unlink a
segment by name without attaching to it, returning `True` on success and, on Windows, always
`True` because a section dies with its last handle.
