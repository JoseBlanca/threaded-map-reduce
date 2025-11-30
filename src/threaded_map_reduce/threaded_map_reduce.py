# Copyright (c) 2025 Jose Blanca
# Licensed under the MIT License. See LICENSE file for details.

from typing import Iterable, Iterator, Callable, TypeVar
import itertools
import threading
import queue
import functools
import concurrent.futures


class _UnusedThread:
    pass


UNUSED_THREAD = _UnusedThread()

orig_map = map


class _ComputingThreadsResults:
    def __init__(self, results_queue, num_computing_threads):
        self.results_queue = results_queue
        self.unused_threads = 0
        self.results_yielded = 0
        self.computing_threads = num_computing_threads

    def get_results(self):
        while True:
            result = self.results_queue.get()
            if result is UNUSED_THREAD:
                self.unused_threads += 1
            else:
                self.results_yielded += 1
                yield result
            if self.results_yielded + self.unused_threads == self.computing_threads:
                self.results_queue.shutdown()
                break


class _ChunkDispenser(Iterator):
    def __init__(self, it, chunk_size):
        self._it = iter(it)
        self._lock = threading.Lock()
        self._chunk_size = chunk_size

    def __next__(self):
        with self._lock:
            # we need to hold the lock while filling the chunk because the iterator is not thread safe
            # I have tried to use a safe iterator created with my ThreadSafeIterator, but
            # it is slower than this
            chunk = list(itertools.islice(self._it, self._chunk_size))
            if not chunk:
                raise StopIteration
            else:
                return chunk


def _map_reduce_chunks_from_chunk_dispenser(
    chunk_dispenser, results_queue, map_fn, reduce_fn
):
    chunk_results = (
        functools.reduce(reduce_fn, orig_map(map_fn, chunk))
        for chunk in chunk_dispenser
    )

    try:
        result = functools.reduce(reduce_fn, chunk_results)
    except TypeError as error:
        if "empty iterable" in str(error):
            result = UNUSED_THREAD
        else:
            raise

    results_queue.put(result)


def map_reduce(
    map_fn,
    reduce_fn,
    iterable: Iterable,
    num_computing_threads: int,
    chunk_size: int = 100,
):
    """
    Apply a function to an iterable in parallel using threads and reduce the results.

    Items are processed in chunks across multiple threads, and each
    chunk's mapped results are reduced into a partial value. Partial values are
    then combined using ``reduce_fn`` until a single final result is obtained.

    The function is intended for workloads where independent computations can be
    mapped in parallel and the results can be combined using an associative
    reduction function. For CPU-bound workloads this is most effective on
    free-threaded Python builds.

    Parameters
    ----------
    map_fn
        Callable applied to each item in ``items``. It must be safe to call
        from multiple threads concurrently.
    reduce_fn
        Callable that combines two mapped results into one. It must be
        associative for correct and predictable behaviour.
    items
        Iterable of input items. It is consumed lazily: items are pulled and
        processed in chunks until the iterable is exhausted.
    num_computing_threads
        Number of worker threads used to process chunks.
    chunk_size
        Number of items grouped into each chunk (default: 100). Small chunk
        sizes increase parallelization overhead (more queue operations and
        context switches), while very large chunk sizes increase memory usage
        and usually do not improve performance beyond a certain point. The
        optimal value is workload-dependent.

    Returns
    -------
    R
        The final reduced value obtained by applying ``reduce_fn`` across all
        mapped results.

    Notes
    -----
    * If ``map_fn`` raises an exception in any worker thread, the exception is
      propagated and the reduction stops.
    * ``reduce_fn`` should be associative, e.g. addition, multiplication,
      minimum, maximum, logical operations, or custom associative functions.
      Non-associative reductions may produce results that depend on chunking
      and processing order.

    Examples
    --------
    Compute the sum of squares:

    >>> from operator import add
    >>> from threaded_map_reduce import map_reduce
    >>> def square(x: int) -> int:
    ...     return x * x
    ...
    >>> nums = range(6)
    >>> map_reduce(square, add, nums, num_computing_threads=4, chunk_size=50)
    55
    """
    items = iter(iterable)
    chunk_dispenser = _ChunkDispenser(items, chunk_size)

    results_queue = queue.Queue()

    map_reduce_items = functools.partial(
        _map_reduce_chunks_from_chunk_dispenser,
        chunk_dispenser=chunk_dispenser,
        map_fn=map_fn,
        reduce_fn=reduce_fn,
        results_queue=results_queue,
    )

    computing_threads = []
    for idx in range(num_computing_threads):
        thread = threading.Thread(target=map_reduce_items, name=f"comp_thread_{idx}")
        thread.start()
        computing_threads.append(thread)

    results = _ComputingThreadsResults(results_queue, num_computing_threads)
    results = results.get_results()

    result = functools.reduce(reduce_fn, results)

    return result


_map_reduce_with_thread_pool_and_buffers = map_reduce


def _threaded_map_with_pool_executor(map_fn, items, max_workers, chunk_size=1):
    with concurrent.futures.ThreadPoolExecutor(max_workers=max_workers) as executor:
        return executor.map(map_fn, items, chunksize=chunk_size)


class _WorkerError:
    def __init__(self, exc):
        self.exc = exc


def _map_chunks_from_chunk_dispenser(chunk_dispenser, results_queue, map_fn):
    put = results_queue.put
    try:
        for idx, chunk in enumerate(chunk_dispenser):
            mapped_chunk = list(orig_map(map_fn, chunk))
            put((idx, mapped_chunk))
    except Exception as exception:
        put(_WorkerError(exception))
    finally:
        put(UNUSED_THREAD)


def _threaded_map_with_chunk_dispenser(
    map_fn,
    items: Iterable,
    num_computing_threads: int,
    chunk_size: int = 100,
    unordered=True,
):
    ordered = not unordered
    items = iter(items)
    chunk_dispenser = _ChunkDispenser(items, chunk_size)

    results_queue = queue.Queue()

    mapped_chunks = functools.partial(
        _map_chunks_from_chunk_dispenser,
        chunk_dispenser=chunk_dispenser,
        map_fn=map_fn,
        results_queue=results_queue,
    )

    computing_threads = []
    for idx in range(num_computing_threads):
        thread = threading.Thread(target=mapped_chunks, name=f"comp_thread_{idx}")
        thread.start()
        computing_threads.append(thread)

    pending_chunks = {}
    next_idx = 0
    num_closed_threads = 0
    while True:
        idx_result = results_queue.get()

        if idx_result is UNUSED_THREAD:
            num_closed_threads += 1
            if num_closed_threads == num_computing_threads:
                # flush any remaining buffered chunks
                if ordered:
                    while next_idx in pending_chunks:
                        yield from pending_chunks.pop(next_idx)
                        next_idx += 1
                break
        else:
            if ordered:
                idx, mapped_chunk = idx_result
                if idx == next_idx:
                    # Emit immediately and then flush any buffered successors
                    yield from mapped_chunk
                    next_idx += 1
                    while next_idx in pending_chunks:
                        yield from pending_chunks.pop(next_idx)
                        next_idx += 1
                else:
                    pending_chunks[idx] = mapped_chunk
            else:
                # No ordering guarantees
                _, mapped_chunk = idx_result
                yield from mapped_chunk


T = TypeVar("T")
R = TypeVar("R")


def map(
    map_fn: Callable[[T], R],
    items: Iterable[T],
    num_computing_threads: int,
    chunk_size: int = 100,
) -> Iterator[R]:
    """
    Apply a function to an iterable in parallel using threads, preserving input order.

    This function behaves like the built-in :func:`map`, but processes items in
    multiple threads and it is intended to be used with the free-threaded Python for
    CPU-bound workloads.

    Items are internally grouped into chunks of size ``chunk_size`` to reduce scheduling
    and queue overhead.

    Parameters
    ----------
    map_fn
        Callable applied to each item in ``items``. It must be safe to call
        from multiple threads concurrently.
    items
        Iterable of input items. It is consumed lazily: items are pulled and
        processed in chunks until the iterable is exhausted.
    num_computing_threads
        Number of worker threads used to process chunks.
    chunk_size
        Number of items grouped into each chunk (default: 100). Small chunk
        sizes increase parallelization overhead (more queue operations and
        context switches), while very large chunk sizes increase memory usage
        and usually do not improve performance beyond a certain point. The
        optimal value is workload-dependent.

    Returns
    -------
    Iterator[R]
        An iterator yielding the results of ``map_fn(item)`` for each item in
        ``items``, in the same order as the original iterable.

    Notes
    -----
    * If ``map_fn`` raises an exception in any worker thread, the exception is
      propagated to the consumer when the corresponding result is requested,
      and iteration stops.
    * For workloads where result ordering does not matter, consider using
      :func:`map_unordered`, which can be slightly more efficient.

    Examples
    --------
    Basic usage:

    >>> from threaded_map_reduce import map as threaded_map
    >>> def square(x: int) -> int:
    ...     return x * x
    ...
    >>> nums = range(10)
    >>> list(threaded_map(square, nums, num_computing_threads=4, chunk_size=50))
    [0, 1, 4, 9, 16, 25, 36, 49, 64, 81]
    """

    return _threaded_map_with_chunk_dispenser(
        map_fn=map_fn,
        items=items,
        num_computing_threads=num_computing_threads,
        chunk_size=chunk_size,
        unordered=False,
    )


def map_unordered(
    map_fn: Callable[[T], R],
    items: Iterable[T],
    num_computing_threads: int,
    chunk_size: int = 100,
) -> Iterator[R]:
    """
    Apply a function to an iterable in parallel using threads, without preserving input order.

    This function behaves like :func:`map`, but results are yielded as soon as
    each chunk is processed, regardless of their original position. For
    workloads where ordering does not matter, this can provide slightly better
    throughput than the ordered :func:`map`.

    As with the ordered variant, items are grouped into chunks of size
    ``chunk_size`` to reduce scheduling and queue overhead, and this function is
    intended to be used with free-threaded Python for CPU-bound tasks.

    Parameters
    ----------
    map_fn
        Callable applied to each item in ``items``. It must be safe to call
        from multiple threads concurrently.
    items
        Iterable of input items. It is consumed lazily: items are pulled and
        processed in chunks until the iterable is exhausted.
    num_computing_threads
        Number of worker threads used to process chunks.
    chunk_size
        Number of items grouped into each chunk (default: 100). Small chunk
        sizes increase parallelization overhead (more queue operations and
        context switches), while very large chunk sizes increase memory usage
        and usually do not improve performance beyond a certain point. The
        optimal value is workload-dependent.

    Returns
    -------
    Iterator[R]
        An iterator yielding the results of ``map_fn(item)`` for each item in
        ``items``, in whatever order results become available.

    Notes
    -----
    * Result order is **not** preserved. The first results yielded correspond to
      whichever chunk finishes first.
    * If ``map_fn`` raises an exception in any worker thread, the exception is
      propagated to the consumer when the corresponding result is requested,
      and iteration stops.

    Examples
    --------
    Basic usage:

    >>> from threaded_map_reduce import map_unordered
    >>> def square(x: int) -> int:
    ...     return x * x
    ...
    >>> nums = range(10)
    >>> sorted(map_unordered(square, nums, num_computing_threads=4, chunk_size=50))
    [0, 1, 4, 9, 16, 25, 36, 49, 64, 81]
    """
    return _threaded_map_with_chunk_dispenser(
        map_fn=map_fn,
        items=items,
        num_computing_threads=num_computing_threads,
        chunk_size=chunk_size,
        unordered=True,
    )
