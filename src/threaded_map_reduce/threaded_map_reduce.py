from typing import Iterable
from concurrent.futures import ThreadPoolExecutor, Future
from itertools import chain
import itertools
import threading
import queue
import functools


def _create_future_from_value(value):
    future = Future()
    future.set_result(value)
    return future


def _next_generation_futures(futures, executor, function):
    print("next generation futures")
    print("futures: ", futures)
    results = (f.result() for f in futures)
    print("results:", results)
    for pair in _pairwise(results):
        if len(pair) == 2:
            print("submiting task:", pair)
            yield executor.submit(function, *pair)
        else:
            yield _create_future_from_value(pair[0])


def _accumulate_futures(initial_futures, executor, function):
    futures = initial_futures

    while True:
        it = iter(futures)
        try:
            first_future = next(it)
        except StopIteration:
            raise ValueError("Cannot reduce empty iterable")

        try:
            second_future = next(it)
        except StopIteration:
            return first_future.result()

        remaining_futures = chain(iter((first_future, second_future)), it)
        futures = _next_generation_futures(remaining_futures, executor, function)


def reduce(function, iterable: Iterable, max_workers: int):
    executor = ThreadPoolExecutor(max_workers=max_workers)

    paired_items = _pairwise(iterable)
    futures = (
        executor.submit(function, *pair)
        if len(pair) == 2
        else _create_future_from_value(pair[0])
        for pair in paired_items
    )

    result = _accumulate_futures(futures, executor, function)
    return result


def map_reduce_old(map_fn, reduce_fn, iterable: Iterable, max_workers: int):
    with ThreadPoolExecutor(max_workers=max_workers) as executor:
        mapped_futures = (executor.submit(map_fn, item) for item in iterable)
        return _accumulate_futures(mapped_futures, executor, reduce_fn)


def _ended(future):
    print("ended", future)


class _MapReducer:
    def __init__(self, map_fn, reduce_fn, iterable, max_workers):
        self.executor = ThreadPoolExecutor(max_workers=max_workers)
        self.max_workers = max_workers

        map_tasks = self._create_map_tasks(map_fn, iterable)
        self.tasks_to_submit = map_tasks
        self.map_futures_done_and_waiting_to_be_processed = queue.Queue()

        self.semaphore = threading.Semaphore(max_workers)
        self.more_tasks_to_submit = True
        self.done_event = threading.Event()

        self.map_sum = 0
        while True:
            while not self.map_futures_done_and_waiting_to_be_processed.empty():
                self.map_sum += (
                    self.map_futures_done_and_waiting_to_be_processed.get().result()
                )
            if self.more_tasks_to_submit:
                self._submit_tasks()
            else:
                break
            self.done_event.wait()

        self.executor.shutdown()

    def _create_map_tasks(self, map_fn, iterable):
        def task_done(future):
            self.map_futures_done_and_waiting_to_be_processed.put(future)
            self.semaphore.release()
            self.done_event.set()

        for item in iterable:
            future = self.executor.submit(map_fn, item)
            future.add_done_callback(task_done)
            yield future

    def _submit_tasks(self):
        futures_created = []
        while True:
            if self.semaphore.acquire(blocking=False):
                try:
                    futures_created.append(next(self.tasks_to_submit))
                except StopIteration:
                    self.more_tasks_to_submit = False
                    return futures_created
            else:
                break
        return futures_created


def map_reduce2(map_fn, reduce_fn, iterable: Iterable, max_workers: int):
    if True:
        map_reducer = _MapReducer(map_fn, reduce_fn, iterable, max_workers)
        return map_reducer.map_sum
    else:
        with ThreadPoolExecutor(max_workers=max_workers) as executor:
            executor.map(map_fn, iterable)
    return
    with ThreadPoolExecutor(max_workers=max_workers) as executor:
        futures = [executor.submit(map_fn, item) for item in iterable]
        print("tasks created", futures)
        for future in futures:
            print("threads active: ", threading.active_count())
            future.add_done_callback(_ended)
            print("waiting")
            future.result()


def _map_reduce_chunk(map_fn, reduce_fn, items):
    mapped_items = map(map_fn, items)
    return functools.reduce(reduce_fn, mapped_items)


def _get_n_items(items, num_items):
    return itertools.islice(items, num_items)


def _materialize_futures(futures_queue):
    while True:
        try:
            future = futures_queue.get()
        except queue.ShutDown:
            break
        yield future.result()


def _put_tasks_in_queue(tasks, executor, futures_queue):
    for task in tasks:
        future = executor.submit(task["function"], *task["args"])
        futures_queue.put(future)


def _shutdown_queue(queue_):
    queue_.shutdown()
    print("closing")


def _map_reduce_chunks(map_fn, reduce_fn, items, num_items_per_chunk, max_workers):
    with ThreadPoolExecutor(max_workers=max_workers) as executor:
        futures_queue = queue.Queue(maxsize=max_workers)

        chunks = _create_chunks(items, num_items_per_chunk)
        tasks = (
            {
                "function": _map_reduce_chunk,
                "args": (
                    map_fn,
                    reduce_fn,
                    chunk,
                ),
            }
            for chunk in chunks
        )
        task_submitting_thread1 = threading.Thread(
            target=_put_tasks_in_queue,
            args=(tasks, executor, futures_queue),
        )
        task_submitting_thread1.start()

        intermediate_results = list(_materialize_futures(futures_queue))
        chunks = _create_chunks(intermediate_results, num_items_per_chunk)
        reduce_tasks = [
            {
                "function": functools.reduce,
                "args": (
                    reduce_fn,
                    chunk,
                ),
            }
            for chunk in chunks
        ]
        closing_task = {"function": _shutdown_queue, "args": (futures_queue,)}
        reduce_tasks.append(closing_task)
        task_submitting_thread2 = threading.Thread(
            target=_put_tasks_in_queue,
            args=(reduce_tasks, executor, futures_queue),
        )
        task_submitting_thread2.start()
        while True:
            try:
                future = futures_queue.get()
                print(future.result())
            except queue.ShutDown:
                print("closing")
                break


def map_reduce3(map_fn, reduce_fn, iterable: Iterable, max_workers: int):
    items = iter(iterable)
    materialized_chunk_len = 10000
    _map_reduce_chunks(map_fn, reduce_fn, items, materialized_chunk_len, max_workers)


def _pairwise(iterable):
    it = iter(iterable)
    while True:
        try:
            a = next(it)
            print("a", a)
        except StopIteration:
            break
        try:
            b = next(it)
            print("b", b)
        except StopIteration:
            yield (a,)
            continue
        yield a, b


def _create_chunks(
    items,
    num_items_per_chunk,
):
    while True:
        chunk = list(_get_n_items(items, num_items_per_chunk))
        if not chunk:
            break
        yield chunk


def _map_reduce_items(items, map_fn, reduce_fn):
    mapped_items = map(map_fn, items)
    return functools.reduce(reduce_fn, mapped_items)


def _send_map_reduce_tasks(map_reduce_fn, items, executor):
    for item in items:
        future = executor.submit(map_reduce_fn, item)
        yield future


def simple_map_reduce(
    map_fn, reduce_fn, iterable: Iterable, max_workers: int, num_items_per_chunk=1000
):
    items = iter(iterable)

    executor = ThreadPoolExecutor(max_workers=max_workers)

    items_chunks = _create_chunks(
        items,
        num_items_per_chunk,
    )
    map_reduce_items = functools.partial(
        _map_reduce_items, map_fn=map_fn, reduce_fn=reduce_fn
    )

    first_round_map_reduce_futures = _send_map_reduce_tasks(
        map_reduce_items, items_chunks, executor
    )
    first_round_map_reduce_results = (
        future.result() for future in first_round_map_reduce_futures
    )

    result = functools.reduce(reduce_fn, first_round_map_reduce_results)
    print(result)
    return result


def _get_items_from_queue(items_queue, results_queue):
    while True:
        try:
            yield items_queue.get()
        except queue.ShutDown:
            break


def _map_reduce_items_from_queue(chunks_queue, results_queue, map_fn, reduce_fn):
    chunk_results = (
        functools.reduce(reduce_fn, map(map_fn, chunk))
        for chunk in _get_items_from_queue(chunks_queue, results_queue)
    )
    result = functools.reduce(reduce_fn, chunk_results)
    results_queue.put(result)


def _feed_chunks(items, num_items_per_chunk, chunks_queue):
    for chunk in _create_chunks(
        items,
        num_items_per_chunk,
    ):
        chunks_queue.put(chunk)
    chunks_queue.shutdown()


def map_reduce_with_thread_pool(
    map_fn, reduce_fn, iterable: Iterable, max_workers: int
):
    num_threads = max_workers
    num_items_per_chunk = 10000

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

    computing_threads = []
    for _ in range(num_threads):
        thread = threading.Thread(target=map_reduce_items)
        thread.start()
        computing_threads.append(thread)

    item_feeder_thread = threading.Thread(
        target=_feed_chunks, args=(items, num_items_per_chunk, chunks_queue)
    )
    item_feeder_thread.start()

    results = []
    while True:
        result = results_queue.get()
        results.append(result)
        if len(results) == len(computing_threads):
            results_queue.shutdown()
            break

    result = functools.reduce(reduce_fn, results)

    return result


map_reduce = map_reduce_with_thread_pool
