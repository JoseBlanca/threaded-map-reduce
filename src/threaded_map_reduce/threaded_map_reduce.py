from typing import Iterable
import itertools
import threading
import queue
import functools
from pathlib import Path


def _get_n_items(items, num_items):
    return itertools.islice(items, num_items)


def _create_chunks(
    items,
    num_items_per_chunk,
):
    while True:
        chunk = list(_get_n_items(items, num_items_per_chunk))
        if not chunk:
            break
        yield chunk


def _get_items_from_queue(items_queue):
    while True:
        try:
            yield items_queue.get()
        except queue.ShutDown:
            break


class _UnusedThread:
    pass


UNUSED_THREAD = _UnusedThread()


def _map_reduce_items_from_queue(chunks_queue, results_queue, map_fn, reduce_fn):
    chunk_results = (
        functools.reduce(reduce_fn, map(map_fn, chunk))
        for chunk in _get_items_from_queue(chunks_queue)
    )
    try:
        result = functools.reduce(reduce_fn, chunk_results)
    except TypeError as error:
        if "empty iterable" in str(error):
            result = UNUSED_THREAD
        else:
            raise

    results_queue.put(result)


def _feed_chunks(items, num_items_per_chunk, chunks_queues):
    chunks_queues_cycle = itertools.cycle(chunks_queues)
    for chunk in _create_chunks(
        items,
        num_items_per_chunk,
    ):
        next(chunks_queues_cycle).put(chunk)
    for chunks_queue in chunks_queues:
        chunks_queue.shutdown()


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


def map_reduce_with_thread_pool(
    map_fn,
    reduce_fn,
    iterable: Iterable,
    num_computing_threads: int,
    num_feeding_queues: int,
    num_items_per_chunk: int,
):
    if num_feeding_queues > num_computing_threads:
        num_feeding_queues = num_computing_threads

    items = iter(iterable)
    chunks_queues = [queue.Queue() for _ in range(num_feeding_queues)]
    results_queue = queue.Queue()

    item_feeder_thread = threading.Thread(
        target=_feed_chunks,
        args=(items, num_items_per_chunk, chunks_queues),
        name="feeder_thread",
    )
    item_feeder_thread.start()

    computing_threads = []
    chunks_queues_cycle = itertools.cycle(chunks_queues)
    for idx in range(num_computing_threads):
        map_reduce_items = functools.partial(
            _map_reduce_items_from_queue,
            map_fn=map_fn,
            reduce_fn=reduce_fn,
            chunks_queue=next(chunks_queues_cycle),
            results_queue=results_queue,
        )

        thread = threading.Thread(target=map_reduce_items, name=f"comp_thread_{idx}")
        thread.start()
        computing_threads.append(thread)

    results = _ComputingThreadsResults(results_queue, num_computing_threads)
    results = results.get_results()

    result = functools.reduce(reduce_fn, results)

    return result


map_reduce = map_reduce_with_thread_pool
