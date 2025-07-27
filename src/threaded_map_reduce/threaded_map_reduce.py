from typing import Iterable
import itertools
import threading
import queue
import functools
from pathlib import Path

DO_PROFILING = True
try:
    import yappi
except ImportError:
    print("yappi not installed, not doing profiling")
    DO_PROFILING = False


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


def _feed_chunks(items, num_items_per_chunk, chunks_queue):
    for chunk in _create_chunks(
        items,
        num_items_per_chunk,
    ):
        chunks_queue.put(chunk)
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
    num_items_per_chunk: int,
):
    if DO_PROFILING:
        yappi.set_clock_type("cpu")

    items = iter(iterable)
    chunks_queue = queue.Queue()
    results_queue = queue.Queue()
    map_reduce_items = functools.partial(
        _map_reduce_items_from_queue,
        map_fn=map_fn,
        reduce_fn=reduce_fn,
        chunks_queue=chunks_queue,
        results_queue=results_queue,
    )

    if DO_PROFILING:
        yappi.start()

    item_feeder_thread = threading.Thread(
        target=_feed_chunks,
        args=(items, num_items_per_chunk, chunks_queue),
        name="feeder_thread",
    )
    item_feeder_thread.start()

    computing_threads = []
    for idx in range(num_computing_threads):
        thread = threading.Thread(target=map_reduce_items, name=f"comp_thread_{idx}")
        thread.start()
        computing_threads.append(thread)

    results = _ComputingThreadsResults(results_queue, num_computing_threads)
    results = results.get_results()

    result = functools.reduce(reduce_fn, results)

    if DO_PROFILING:
        yappi.stop()
        profiling_dir = (
            Path(__name__).parent.parent.absolute() / "performance" / "profiling"
        )
        profiling_dir.mkdir(exist_ok=True)
        for thread_stat in yappi.get_thread_stats():
            print(thread_stat.name, thread_stat.id, thread_stat.ttot)
            fpath = profiling_dir / f"thread_{thread_stat.name}_{thread_stat.id}.pstat"
            yappi.get_func_stats(ctx_id=thread_stat.id).save(fpath, type="pstat")
            fpath = (
                profiling_dir / f"thread_{thread_stat.name}_{thread_stat.id}.callgrind"
            )
            yappi.get_func_stats(ctx_id=thread_stat.id).save(fpath, type="callgrind")
        return result
        threads = [item_feeder_thread] + computing_threads
        for thread in threads:
            print(f"Stats for {thread.name}")

    return result


map_reduce = map_reduce_with_thread_pool
