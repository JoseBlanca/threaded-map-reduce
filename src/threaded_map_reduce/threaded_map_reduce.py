from typing import Iterable, Iterator
import itertools
import threading
import queue
import functools
import concurrent.futures


class _UnusedThread:
    pass


UNUSED_THREAD = _UnusedThread()


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
        functools.reduce(reduce_fn, map(map_fn, chunk)) for chunk in chunk_dispenser
    )

    try:
        result = functools.reduce(reduce_fn, chunk_results)
    except TypeError as error:
        if "empty iterable" in str(error):
            result = UNUSED_THREAD
        else:
            raise

    results_queue.put(result)


def _map_reduce_with_thread_pool_and_buffers(
    map_fn,
    reduce_fn,
    iterable: Iterable,
    num_computing_threads: int,
    chunk_size: int = 100,
):
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


map_reduce = _map_reduce_with_thread_pool_and_buffers


def threaded_map_with_pool_executor(map_fn, items, max_workers, chunk_size=1):
    with concurrent.futures.ThreadPoolExecutor(max_workers=max_workers) as executor:
        return executor.map(map_fn, items, chunksize=chunk_size)


def _map_chunks_from_chunk_dispenser(chunk_dispenser, results_queue, map_fn):
    put = results_queue.put
    for chunk in chunk_dispenser:
        mapped_chunk = list(map(map_fn, chunk))
        put(mapped_chunk)
    put(UNUSED_THREAD)


def threaded_map_with_chunk_dispenser(
    map_fn, items: Iterable, num_computing_threads: int, chunk_size: int = 100
):
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

    num_closed_threads = 0
    while True:
        result = results_queue.get()
        if result is UNUSED_THREAD:
            num_closed_threads += 1
            if num_closed_threads == num_computing_threads:
                results_queue.shutdown()
                break
        else:
            yield from result


threaded_map = threaded_map_with_chunk_dispenser
