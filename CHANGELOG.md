# Changelogs

## [v2.0.0] - 2026-09-13

Tracks C++ [slick-stream-buffer-multiplexer v2.0.0](https://github.com/SlickQuant/slick-stream-buffer-multiplexer/blob/main/CHANGELOG.md)
and moves to the version number shared with the C++ header and the sibling ports (0.1.0 -> 2.0.0).
The shared-memory formats are unchanged, so segments written by v0.1.0 (or a C++ v1.x process)
are still readable with the default traits.

### Breaking
- Requires **slick-queue-py >= 2.0.0** and **slick-stream-buffer-py >= 2.0.0**.
- `ProducerBuffer.loss_count()` stays at 0 under multiplexer traffic. Records are dereferenced
  without the inner ring's scan-loss counter, which for a jump-read would count the same lap
  again on every later record; the multiplexer counts one loss per lapped record in
  `mux.loss_count()` instead. The inner counter only moves for reads made through
  `producer.stream_buffer.read(cursor, traits)` with counting traits.
- `close()` keeps the registration table, so `has_producer()` and `producer_count()` still
  report closed producers - `unlink()` after `close()` needs them (see Fixed).

### Added
- `traits` constructor argument: slick-queue-py's `QueueTraits`, passed straight through to the
  shared record queue and readable back as `mux.traits`; `QueueTraits` and
  `default_queue_traits` are re-exported. `enable_loss_detection` (default on, as v0.1.0 always
  counted) switches both terms of `loss_count()` together. `enable_read_last` is recorded in the
  queue's header (`'SLQ1'` on, `'SLQ0'` off) and must match on every Python and C++ peer, or
  attaching fails with "Shared memory feature mismatch".
- `StreamBufferMultiplexer.remove(name)`: unlink a queue or producer segment by name, to recover
  from a run that died mid-initialization. `tests/cleanup_shm.py` uses it.
- `get_producer_buffers()`: a read-only live view of the registration table, keyed by
  `producer_id`.
- Tests: traits accessor and validation, loss counting disabled by traits, scan loss vs
  multiplexer loss, `producer_id` range, `get_producer_buffers()`, `remove()`, `unlink()` after
  `close()`, and C++/Python interop with `enable_read_last = False` whichever language creates the
  queue (`cpp_mux_producer` / `cpp_mux_consumer --no-read-last`).

### Fixed
- `add_producer()` validates `producer_id` against the record's `uint32` field (`TypeError` for a
  non-integer, `ValueError` out of range) before creating any segment. An out-of-range id made
  `consume()` raise `struct.error` after the producer's ring had already published.
- `unlink()` after `close()` - the documented order - leaked every segment on POSIX, because
  `close()` emptied the registration table and dropped the queue's `SharedMemory` handle. Both are
  kept now, and repeating `unlink()` is a no-op.
- `loss_count()` documentation: only the multiplexer-level term is filtered by producer
  registration. Shared-queue wrap loss can include producers this instance never registered, so
  the total is an upper bound.

### Changed
- The interop tests build against C++ slick-stream-buffer-multiplexer `v2.0.0` (was `main`) and
  require CMake 3.21.

## [v0.1.0] - 2026-07-09

### Added
- Initial release: Python implementation of slick-stream-buffer-multiplexer, a lock-free
  MPMC byte stream multiplexer, binary-compatible with the C++ version
  (`slick/stream_buffer_multiplexer.hpp` v1.0.x) over shared memory
- `StreamBufferMultiplexer` with the full C++ API surface:
  - Construction: local / shared-memory-creator / shared-memory-opener record queue
  - `add_producer()` in all three C++ overload forms (local, shm creator, shm opener)
  - `read(cursor)` with both a per-consumer int cursor and an `AtomicCursor`
    (work-stealing) — records from unregistered producers silently skipped
  - Three-tier loss model: shared-queue wrap loss + multiplexer-level loss
    (`loss_count()`), per-producer inner-ring loss (`ProducerBuffer.loss_count()`)
  - `initial_reading_index()`, `has_producer()`, `get_producer_buffer()`,
    `find_producer()`, `producer_count()`
  - Lifecycle: `close()`, `unlink()`, `get_shm_name()`, context manager support
- `ProducerBuffer` forwarding the full producer API (`prepare`/`commit`/`consume`/
  `discard`/`data`/`size`) with `consume()` fanning a 16-byte
  `{sequence, producer_id}` record into the shared queue
- Built on slick-queue-py >= 1.2.0 (shared record queue) and
  slick-stream-buffer-py >= 0.1.0 (per-producer buffers); pure Python, no new
  C++ extension
- Test suites: basic (9), MPMC stress incl. work-stealing (4), shared memory incl.
  cross-process (6), and C++ interop tests (4) that exchange messages with real C++
  binaries built against the actual header — including concurrent mixed-language
  producers fanning into the same shared queue
- CMake/ctest wiring with FetchContent (local source overrides supported), CI workflow,
  release workflows, README, API_DIFFERENCES.md
