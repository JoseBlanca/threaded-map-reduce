# Changelog

## [0.1.2] - 2026-09-18

### Fixed
- `map` dropped results without a word when the work reached more than one
  thread. Every worker enumerated the chunk dispenser on its own, so the index
  that puts the mapped chunks back in order was local to the thread, and the
  chunks that two threads numbered alike overwrote each other in the pending
  chunks. With four threads and an iterable slow enough to go through, a third
  of the results came out. The index is given now by the dispenser, while it
  holds the lock, so it says what it is meant to say about the order.
  `map_unordered` and `map_reduce` were not affected, neither of them uses the
  index.

### Added
- Tests that go over an iterable that is slow to pull from, so that the work is
  spread over the threads. The ones that go over a `range` are so fast that one
  thread tends to take every chunk, which hides this kind of bug.

## [0.1.1] - 2025-11-30

### Added
- Detailed performance documentation for `map` and `map_reduce`, including
  benchmarks on free-threaded CPython 3.14.0t and charts.
- New section explaining the impact of `chunk_size` on performance and memory usage.
- Docstrings for `map`, `map_unordered`, and `map_reduce` describing behavior,
  parameters, error propagation, and usage examples.

### Changed
- Improved README: clarified API summary, performance sections, and comparison
  with `concurrent.futures.ThreadPoolExecutor.map`.
- Polished wording in documentation and better explain ideal scaling and
benchmark assumptions.

### Fixed
- Typographical errors and small inconsistencies in README and comments.

## [0.1.0] - 2025-11-27

### Added
- Initial public release on PyPI.
- `map` with ordered parallel mapping.
- `map_unordered` with unordered parallel mapping.
- `map_reduce` for threaded map+reduce on chunked workloads.