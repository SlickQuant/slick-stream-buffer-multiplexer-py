# API Differences: Python vs C++

The Python implementation is binary-compatible with the C++
`slick::stream_buffer_multiplexer` at the shared memory level — the shared record queue
('SLQ1', 16-byte elements) and every producer segment ('SSB1') match byte-for-byte. The
*language-level* API differs in the following ways.

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

## loss_count()

C++ compiles the multiplexer-level counter out unless
`SLICK_STREAM_BUFFER_MULTIPLEXER_ENABLE_LOSS_DETECTION` is enabled (debug default);
Python always counts. Same for the shared queue's wrap-loss term (slick-queue-py always
counts). All counters are per-instance, not shared through the segments.

## Lifecycle

Python adds explicit lifecycle management that C++ handles via RAII:

- `close()` — detach from all producer segments and the queue segment
- `unlink()` — delete the queue segment and all registered producers' segments
- context manager support: `with StreamBufferMultiplexer(...) as mux: ...`
- `get_shm_name()` — on the multiplexer (queue segment) and on each `ProducerBuffer`,
  the exact names to pass to C++ (`/`-prefixed on POSIX)
