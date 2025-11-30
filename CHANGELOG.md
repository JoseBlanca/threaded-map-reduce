# Changelog

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