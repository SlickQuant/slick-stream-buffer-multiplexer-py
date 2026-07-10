# Changelogs

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
