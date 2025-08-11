from concurrent.futures import ThreadPoolExecutor
from functools import reduce, partial
import itertools


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


def _map_reduce_chunk(chunk, map_fn, reduce_fn, initial_reduce_value=None):
    return reduce(reduce_fn, map(map_fn, chunk), initial_reduce_value)


def map_reduce_with_executor(
    map_fn,
    reduce_fn,
    items,
    max_workers,
    num_items_per_chunk,
    initial_reduce_value=None,
):
    items = iter(items)
    chunks = _create_chunks(items, num_items_per_chunk)
    map_reduce_chunk = partial(
        _map_reduce_chunk,
        map_fn=map_fn,
        reduce_fn=reduce_fn,
        initial_reduce_value=initial_reduce_value,
    )

    with ThreadPoolExecutor(max_workers=max_workers) as executor:
        futures = []
        reduced_result = initial_reduce_value
        for chunk in chunks:
            future_reduced_result = executor.submit(map_reduce_chunk, chunk)
            futures.append(future_reduced_result)

            if len(futures) > max_workers:
                future = futures.pop()
                reduced_result_for_chunk = future.result()
                reduced_result = reduce_fn(reduced_result_for_chunk, reduced_result)

        for future in futures:
            reduced_result = reduce_fn(future.result(), reduced_result)
    return reduced_result


def map_reduce_with_executor_naive(
    map_fn,
    reduce_fn,
    items,
    max_workers,
    initial_reduce_value=None,
):
    with ThreadPoolExecutor(max_workers=max_workers) as executor:
        futures = []
        reduced_result = initial_reduce_value
        for item in items:
            future = executor.submit(map_fn, item)
            futures.append(future)

            if len(futures) > max_workers:
                future = futures.pop()
                result = future.result()
                reduced_result = reduce_fn(result, reduced_result)

        for future in futures:
            reduced_result = reduce_fn(future.result(), reduced_result)
    return reduced_result


if __name__ == "__main__":
    reduce_fn = lambda x, y: x + y
    map_fn = lambda x: x**2
    items = range(100000)
    result1 = reduce(reduce_fn, map(map_fn, items))
    items = range(100000)
    if True:
        result2 = map_reduce_with_executor(
            map_fn,
            reduce_fn,
            items,
            max_workers=2,
            initial_reduce_value=0,
            num_items_per_chunk=1000,
        )
    if False:
        result2 = map_reduce_with_executor_naive(
            map_fn,
            reduce_fn,
            items,
            max_workers=2,
            initial_reduce_value=0,
        )
    print(result1)
    print(result2)
