from typing import Iterable, Iterator
import itertools
import threading
import queue
import functools


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
    buffer_size: int,
):
    items = iter(iterable)
    chunk_dispenser = _ChunkDispenser(items, buffer_size)

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
