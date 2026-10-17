# Changelogs

## [v2.2.0] - 2026-10-14

Tracks C++ [slick-stream-buffer-multiplexer v2.2.0](https://github.com/SlickQuant/slick-stream-buffer-multiplexer/blob/main/CHANGELOG.md):
a second kind of producer, a byte slick-queue, merged into the same globally ordered stream. The
version moves from 2.0.0 to 2.2.0 to stay on the C++ number. C++ v2.1.0 changed no code of its
own - its segment-lifetime change is in its C++ dependencies - and that change reaches this
package the same way, through slick-queue-py 2.3.0 and slick-stream-buffer-py 2.1.0 (see Segment
lifetime). The shared-memory formats are unchanged.

### Segment lifetime (behavior change)
- On POSIX, a segment is no longer unlinked when the process that created it exits, nor - before
  Python 3.13 - when a process that only attached to it exits. Both happened through
  `multiprocessing`'s resource tracker, which the dependencies now keep their segments out of. A
  producer that exited first therefore orphaned every consumer still attached, and its restart
  silently created a fresh segment under the same name that those consumers never saw. A segment
  now lives until `unlink()` or `remove(name)`, so a restarted producer reattaches to it and
  keeps its contents, as in C++ 2.1.0 and later. Windows is unaffected: a section dies with its
  last handle.
- Migration: code that relied on that cleanup now leaves segments in `/dev/shm` and attaches to
  them on its next run - `own_buffer` is `False`, and a creator with a different geometry raises.
  Call `unlink()` from the process that coordinates the segments' lifetime at shutdown, or
  `remove(name)` at startup.

### Added
- Queue producers. `add_queue_producer()` registers a byte `SlickQueue` (`element_size=1`, the C++
  `slick::queue<uint8_t>`), built with the multiplexer's traits: in local memory
  (`size=`, optionally `items_per_slot=`), as a shared-memory creator (add `name=`) or opener
  (`name=` alone; size and `items_per_slot` come from the segment), or by adopting a byte
  `SlickQueue` you already have (`queue=`). Only messages published through the returned handle
  are multiplexed.
- `ProducerQueue`, the queue producer's handle: `reserve()`, a writable `pq[index]` view and
  `publish()`, with `publish()` also fanning a `{index, producer_id}` record into the shared queue.
  Unlike a `ProducerBuffer`, any number of threads may publish through one. It also has `size`,
  `items_per_slot`, `loss_count()`, `initial_reading_index()`, `own_buffer`, `use_shm`, `queue`,
  `get_shm_name()`, `close()` and `unlink()`.
- `get_producer_queue()`, `find_producer_queue()` and `get_producer_queues()`, the queue
  counterparts of `get_producer_buffer()`, `find_producer()` and `get_producer_buffers()`.
- `ProducerKind` (`STREAM_BUFFER` / `QUEUE`), as `.kind` on both handles, which now share a common
  base with `producer_id`.
- Tests: a queue producer suite (`tests/test_multiplexer_queue.py`, 19 tests, registered with
  CTest), multi-writer and work-stealing MPMC tests, shared-memory tests including a
  cross-process roundtrip, C++/Python interop with a queue producer in each direction
  (`cpp_mux_producer create-queue-producer` / `--queue-producer`, `cpp_mux_consumer
  --queue-producer`), and segment lifetime: `unlink()` leaves the resource tracker alone, and
  neither a creator's nor an attacher's exit removes the record queue or either kind of producer
  segment.

### Changed
- Requires **slick-queue-py >= 2.3.0** (was 2.0.0), for `items_per_slot` and segment lifetime,
  and **slick-stream-buffer-py >= 2.1.0** (was 2.0.0), for segment lifetime.
- `producer_id` is one namespace across both kinds: registering an id already held by either kind
  raises `ValueError`. `has_producer()` and `producer_count()` count both kinds, while
  `get_producer_buffer()`, `find_producer()` and `get_producer_buffers()` keep returning stream
  buffer producers only - `None` for a queue producer's id.
- `close()`, `unlink()` and `remove()` cover queue producer segments as well.
- A consumer on this package before 2.2.0, or on C++ 2.1 or earlier, can share a record queue
  with a 2.2 process. It has no queue producers, so it skips their records as unregistered,
  without counting them as loss.
- The interop tests build against C++ slick-stream-buffer-multiplexer `v2.2.0` (was `v2.0.0`).

### Queue producer loss
- A queue message whose bytes may have been overwritten is skipped and counted, not only one
  whose control slot was lapped: messages differ in length, so a later reservation can cover an
  older message's bytes without landing on its slot. Once the queue's reservation counter is more
  than one ring past a message, the message is treated as lost. `read()` copies the bytes before
  the check, so the check covers the copy it returns. It is conservative: a message lying in the
  tail a wrapping reservation skipped is reported lost although its bytes are intact.
- `ProducerQueue.loss_count()` is the inner queue's own counter and is not a record count:
  slick-queue configures loss detection per queue, not per call, so with counting traits (the
  slick-queue-py default) each lapped dereference adds slick-queue's skip distance to it.
  `mux.loss_count()` remains the number to read.

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
