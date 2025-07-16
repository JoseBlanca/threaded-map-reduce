from typing import Iterable
from concurrent.futures import ThreadPoolExecutor, Future


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


def _accumulate_futures(initial_futures, executor, function):
    futures = list(initial_futures)

    while len(futures) > 1:
        results = (future.result() for future in futures)
        paired_items = _pairwise(results)

        new_futures = []
        for pair in paired_items:
            if len(pair) == 2:
                new_futures.append(executor.submit(function, *pair))
            else:
                # single leftover, wrap it in a completed future
                new_futures.append(_create_future_from_value(pair[0]))
        futures = new_futures

    final_result = futures[0].result()
    return final_result


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
