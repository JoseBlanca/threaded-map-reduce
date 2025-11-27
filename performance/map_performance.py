from time import time

import numpy

from threaded_map_reduce.threaded_map_reduce import (
    _threaded_map_with_chunk_dispenser,
    _threaded_map_with_pool_executor,
)
from performance_utils import is_prime


def do_non_threaded_experiment(
    num_threadss: list[int],
    chunk_sizes: list[int],
    mapping_fn,
    threaded_mapping_fn,
    items_to_map,
    num_threads_arg_name,
):
    time_start = time()
    non_threaded_result = list(map(mapping_fn, items_to_map))
    for item_result in non_threaded_result:
        item_result

    time_used = time() - time_start
    return {"time": time_used}


def do_threaded_experiment(
    num_threadss: list[int],
    chunk_sizes: list[int],
    mapping_fn,
    threaded_mapping_fn,
    items_to_map,
    num_threads_arg_name,
):
    results = []
    for chunk_size in chunk_sizes:
        times = []
        for num_threads in num_threadss:
            time_start = time()
            kwagrs = {
                "map_fn": mapping_fn,
                "items": items_to_map,
                "chunk_size": chunk_size,
                num_threads_arg_name: num_threads,
            }
            threaded_result = threaded_mapping_fn(**kwagrs)

            for item_result in threaded_result:
                item_result

            # print(threaded_result[:10])
            time_used = time() - time_start
            times.append(time_used)
        result = {
            "chunk_size": chunk_size,
            "times": numpy.array(times),
            "num_threadss": num_threadss,
        }
        results.append(result)
        print(result)
    return results


def check_map_performance_with_primes():
    num_numbers_to_check = 1000000
    # num_numbers_to_check = 100000
    # num_numbers_to_check = 50000
    chunk_sizes = (1000, 100, 1)
    chunk_sizes = (100,)
    num_threadss = list(range(1, 17))
    mapping_fn = is_prime

    experiment1 = {
        "num_threadss": num_threadss,
        "chunk_sizes": chunk_sizes,
        "mapping_fn": mapping_fn,
        "items_to_map": range(1, num_numbers_to_check),
        "num_threads_arg_name": "max_workers",
        "threaded_mapping_fn": _threaded_map_with_pool_executor,
    }
    experiment2 = {
        "num_threadss": num_threadss,
        "chunk_sizes": chunk_sizes,
        "mapping_fn": mapping_fn,
        "items_to_map": range(1, num_numbers_to_check),
        "num_threads_arg_name": "num_computing_threads",
        "threaded_mapping_fn": _threaded_map_with_chunk_dispenser,
    }
    experiment = experiment2
    result = do_non_threaded_experiment(**experiment)
    print(f"time non threaded: {result['time']}")
    results = do_threaded_experiment(**experiment)
    print(results)


if __name__ == "__main__":
    check_map_performance_with_primes()
