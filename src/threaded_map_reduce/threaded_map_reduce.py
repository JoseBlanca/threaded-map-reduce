from typing import Iterable
from concurrent.futures import ThreadPoolExecutor, Future
from itertools import chain
import threading
import queue


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


def map_reduce(map_fn, reduce_fn, iterable: Iterable, max_workers: int):
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
