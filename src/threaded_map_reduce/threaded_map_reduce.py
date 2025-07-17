from typing import Iterable
from concurrent.futures import ThreadPoolExecutor, Future
from itertools import chain


def _pairwise(iterable):
    it = iter(iterable)
    while True:
        try:
            a = next(it)
        except StopIteration:
            break
        try:
            b = next(it)
        except StopIteration:
            yield (a,)
            continue
        yield a, b


def _create_future_from_value(value):
    future = Future()
    future.set_result(value)
    return future


def _next_generation_futures(futures, executor, function):
    results = (f.result() for f in futures)
    for pair in _pairwise(results):
        if len(pair) == 2:
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


def map_reduce(map_fn, reduce_fn, iterable: Iterable, max_workers: int):
    with ThreadPoolExecutor(max_workers=max_workers) as executor:
        mapped_futures = (executor.submit(map_fn, item) for item in iterable)
        print("mapped")
        return _accumulate_futures(mapped_futures, executor, reduce_fn)
