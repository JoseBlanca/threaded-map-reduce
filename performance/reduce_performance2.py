from time import time
from functools import reduce, partial
from operator import add
from pathlib import Path
import sys

import numpy
import matplotlib.pyplot as plt

from threaded_map_reduce import map_reduce as map_reduce_with_thread_pool_and_buffers
from other_implementations import (
    map_reduce_naive,
    map_reduce_with_thread_pool_no_feeding_queue,
)


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
    chunk_size_argument_name,
):
    non_threaded_result = count_primes_non_threaded(num_numbers_to_check)
    result = {
        "non_threaded_time": non_threaded_result["time"],
        "results_for_different_chunk_sizes": [],
    }
    non_threaded_result = non_threaded_result["result"]

    for num_items_per_chunk in num_items_per_chunks:
        kwargs = {}
        if chunk_size_argument_name:
            kwargs[chunk_size_argument_name] = num_items_per_chunk

        this_map_reduce_funct = partial(map_reduce_funct, **kwargs)

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


def get_python_version():
    version_info = sys.version_info
    version = f"{version_info.major}.{version_info.minor}.{version_info.micro}"
    if "free-threading" in sys.version:
        version += "t"
    return version


def plot_results(experiment_name, results, charts_dir, chunk_size_argument_name):
    non_threaded_time = results["non_threaded_time"]
    nice_experiment_name = experiment_name.capitalize().replace("_", " ")

    base_fname = f"{get_python_version()}"
    for result in results["results_for_different_chunk_sizes"]:
        chunk_size = result["chunk_size"]
        if chunk_size_argument_name:
            title = f"{nice_experiment_name}, {chunk_size_argument_name}: {chunk_size}"
            this_base_fname = f"{base_fname}.{chunk_size_argument_name}_{chunk_size}"
        else:
            title = f"{nice_experiment_name}"
            this_base_fname = f"{base_fname}"

        plot_path = charts_dir / f"{this_base_fname}.time.png"
        fig, axes = plt.subplots()
        axes.plot(
            result["n_threads"],
            result["times"],
            linestyle="-",
            marker="o",
            color="blue",
            label="threaded",
        )
        ideal_times = non_threaded_time / result["n_threads"]
        axes.plot(
            result["n_threads"],
            ideal_times,
            linestyle="-",
            marker="o",
            color="grey",
            label="ideal",
        )
        xmin, xmax = axes.get_xlim()
        axes.hlines(
            non_threaded_time,
            xmin=xmin,
            xmax=xmax,
            color="red",
            label="non_threaded",
        )
        axes.set_ylim(bottom=0, top=axes.get_ylim()[1])
        axes.set_ylabel("Time (s)")
        axes.set_xlabel("Num. threads")
        axes.set_title(title)
        axes.legend()
        fig.savefig(plot_path)

        speedup = non_threaded_time / result["times"]
        efficiency = speedup / result["n_threads"]
        plot_path = charts_dir / f"{this_base_fname}.efficiency.png"
        fig, axes = plt.subplots()
        axes.plot(
            result["n_threads"],
            efficiency,
            linestyle="-",
            marker="o",
            color="blue",
        )
        axes.set_ylim(bottom=0, top=axes.get_ylim()[1])
        axes.set_ylabel("Efficiency")
        axes.set_xlabel("Num. threads")
        axes.set_title(title)
        fig.savefig(plot_path)


def check_performance_with_primes():
    num_numbers_to_check = 1000000
    num_numbers_to_check = 100000
    num_items_per_chunks = (1000, 100, 1)
    num_threadss = list(range(1, 17))

    # Iterator is chunked.
    # While a chunk is being created a lock is put in the iterator because iterators are not thread safe
    # Chunks are lists
    # a pool of computing threads is created
    # each thread computes a chunk at a time
    # results are returned by the threads in a queue
    map_reduce_funct = map_reduce_with_thread_pool_and_buffers
    experiment_name = "thread_pool_and_buffers"
    chunk_size_argument_name = "buffer_size"

    # The iterator is made thread safe by locking while getting each next item
    # a pool of computing threads is created
    # each thread computes an item at a time
    # results are returned by the threads in a queue
    map_reduce_funct = map_reduce_naive
    experiment_name = "naive_map_reduce"
    chunk_size_argument_name = None

    # the chunks are islices
    # the chunks iterator is made thread safe by putting it inside a ThreadSafeIterator
    # a pool of computing threads is created
    # each thread computes a chunk at a time
    # results are returned by the threads in a queue
    map_reduce_funct = map_reduce_with_thread_pool_no_feeding_queue
    experiment_name = "thread_pool_no_feeding_queue"
    chunk_size_argument_name = "num_items_per_chunk"

    if chunk_size_argument_name is None:
        num_items_per_chunks = (1,)

    base_charts_dir = Path(__file__).parent / "charts"
    base_charts_dir.mkdir(exist_ok=True)
    charts_dir = base_charts_dir / "primes"
    charts_dir.mkdir(exist_ok=True)
    charts_dir = charts_dir / f"num_numbers_{num_numbers_to_check}"
    charts_dir.mkdir(exist_ok=True)
    charts_dir = charts_dir / f"{experiment_name}"
    charts_dir.mkdir(exist_ok=True)

    results = do_prime_experiment_with_several_chunk_sizes(
        num_numbers_to_check,
        num_items_per_chunks,
        num_threadss,
        map_reduce_funct,
        chunk_size_argument_name,
    )

    plot_results(
        experiment_name,
        results,
        charts_dir=charts_dir,
        chunk_size_argument_name=chunk_size_argument_name,
    )


if __name__ == "__main__":
    check_performance_with_primes()
