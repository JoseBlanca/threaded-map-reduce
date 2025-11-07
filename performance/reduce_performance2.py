from time import time
from functools import reduce, partial
from operator import add

import numpy

from threaded_map_reduce import map_reduce as map_reduce_with_thread_pool_and_buffers


def is_prime(n):
    if n == 2 or n == 3:
        return True
    if n < 2 or n % 2 == 0:
        return False
    if n < 9:
        return True
    if n % 3 == 0:
        return False
    r = int(n**0.5)
    # since all primes > 3 are of the form 6n ± 1
    # start with f=5 (which is prime)
    # and test f, f+2 for being prime
    # then loop by 6.
    f = 5
    while f <= r:
        if n % f == 0:
            return False
        if n % (f + 2) == 0:
            return False
        f += 6
    return True


def count_primes_non_threaded(num_numbers):
    numbers = range(1, num_numbers)
    start_time = time()

    total = reduce(add, map(is_prime, numbers))

    time_used = time() - start_time
    return {"time": time_used, "result": total}


def count_primes_threaded(
    num_numbers,
    num_computing_threads,
    map_reduce_funct,
):
    numbers = range(1, num_numbers)
    start_time = time()
    total = map_reduce_funct(
        is_prime,
        add,
        numbers,
        num_computing_threads=num_computing_threads,
    )
    time_used = time() - start_time
    return {"time_used": time_used, "result": total}


def check_count_primes_performance(
    numbers_to_check,
    num_threadss,
    map_reduce_funct,
):
    times_used = []
    results = []
    for num_threads in num_threadss:
        res = count_primes_threaded(
            numbers_to_check,
            num_threads,
            map_reduce_funct,
        )
        times_used.append(res["time_used"])
        results.append(res["result"])
    return {
        "n_threads": numpy.array(num_threadss),
        "times": numpy.array(times_used),
        "results": numpy.array(results),
    }


def do_prime_experiment_with_several_chunk_sizes(
    num_numbers_to_check,
    num_items_per_chunks,
    num_threadss,
    map_reduce_funct,
):
    non_threaded_result = count_primes_non_threaded(num_numbers_to_check)
    result = {
        "non_threaded_time": non_threaded_result["time"],
        "results_for_different_chunk_sizes": [],
    }
    non_threaded_result = non_threaded_result["result"]
    for num_items_per_chunk in num_items_per_chunks:
        this_map_reduce_funct = partial(
            map_reduce_funct, buffer_size=num_items_per_chunk
        )
        res = check_count_primes_performance(
            num_numbers_to_check,
            num_threadss,
            this_map_reduce_funct,
        )
        assert numpy.all(res["results"] == non_threaded_result)
        this_result = {
            "chunk_size": num_items_per_chunk,
            "n_threads": res["n_threads"],
            "times": res["times"],
        }
        result["results_for_different_chunk_sizes"].append(this_result)
    return result


def check_performance_with_primes():
    num_numbers_to_check = 1000000
    num_items_per_chunks = (1000, 100, 1)
    num_threadss = list(range(1, 17))
    map_reduce_funct = map_reduce_with_thread_pool_and_buffers
    do_prime_experiment_with_several_chunk_sizes(
        num_numbers_to_check,
        num_items_per_chunks,
        num_threadss,
        map_reduce_funct,
    )


if __name__ == "__main__":
    check_performance_with_primes()
