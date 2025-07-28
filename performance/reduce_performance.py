from operator import add
from functools import reduce
from pathlib import Path
import sys
from time import time

import matplotlib.pyplot as plt
import numpy

from threaded_map_reduce.threaded_map_reduce import (
    _map_reduce_with_thread_pool_with_feeding_queues,
    _map_reduce_with_thread_pool_no_feeding_queue,
    _map_reduce_with_thread_pool_and_buffers,
    _map_reduce_naive,
)


def square(num):
    return num**2


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


def count_prime_numbers_in_range(range_):
    return sum(is_prime(num) for num in range_)


def count_primes_standard(num_numbers):
    numbers = range(1, num_numbers)
    start_time = time()

    total = reduce(add, map(is_prime, numbers))

    time_used = time() - start_time
    return {"time_used": time_used, "result": total}


def count_primes_threaded(
    num_numbers,
    num_computing_threads,
    num_items_per_chunk,
    num_feeding_queues,
    map_reduce_funct,
):
    numbers = range(1, num_numbers)
    start_time = time()

    funct_name = map_reduce_funct.__name__

    if funct_name == "map_reduce_with_thread_pool_and_buffers":
        total = map_reduce_with_thread_pool_and_buffers(
            is_prime,
            add,
            numbers,
            num_computing_threads=num_computing_threads,
            buffer_size=num_items_per_chunk,
        )
    elif num_feeding_queues:
        total = map_reduce_with_thread_pool_with_feeding_queues(
            is_prime,
            add,
            numbers,
            num_computing_threads=num_computing_threads,
            num_items_per_chunk=num_items_per_chunk,
            num_feeding_queues=num_feeding_queues,
        )
    else:
        total = map_reduce_with_thread_pool_no_feeding_queue(
            is_prime,
            add,
            numbers,
            num_computing_threads=num_computing_threads,
            num_items_per_chunk=num_items_per_chunk,
        )

    time_used = time() - start_time
    return {"time_used": time_used, "result": total}


def add_squares_standard(num_numbers_to_add):
    numbers = range(num_numbers_to_add)
    start_time = time()

    total = reduce(add, map(square, numbers))

    time_used = time() - start_time
    return {"time_used": time_used, "result": total}


def add_squares_threaded(
    num_numbers_to_add, num_computing_threads, num_items_per_chunk
):
    numbers = range(num_numbers_to_add)
    start_time = time()

    total = map_reduce(
        square,
        add,
        numbers,
        num_computing_threads=num_computing_threads,
        num_items_per_chunk=num_items_per_chunk,
    )

    time_used = time() - start_time
    return {"time_used": time_used, "result": total}


def check_add_numbers_performance():
    numbers_to_add = 20000000
    res = add_squares_standard(numbers_to_add)
    print("standard: ", res["time_used"], res["result"])
    for num_items in range(50, 450, 50):
        res = add_squares_threaded(numbers_to_add, 4, num_items)
        print(f"threaded, num_items ({num_items}): ", res["time_used"], res["result"])

    for num_threads in range(1, 6):
        res = add_squares_threaded(numbers_to_add, num_threads, 1000)
        print(
            f"threaded, num_threads ({num_threads}): ", res["time_used"], res["result"]
        )


def check_count_primes_performance(
    numbers_to_check,
    num_items_per_chunk,
    num_threadss,
    num_feeding_queues,
    map_reduce_funct,
):
    times_used = []
    results = []
    for num_threads in num_threadss:
        res = count_primes_threaded(
            numbers_to_check,
            num_threads,
            num_items_per_chunk,
            num_feeding_queues,
            map_reduce_funct,
        )
        times_used.append(res["time_used"])
        results.append(res["result"])
    return {
        "n_threads": numpy.array(num_threadss),
        "times_used": numpy.array(times_used),
        "results": numpy.array(results),
    }


def do_prime_experiment(
    num_numbers_to_check,
    num_items_per_chunks,
    num_threads,
    num_feeding_queues,
    map_reduce_funct,
):
    funct_name = map_reduce_funct.__name__
    if funct_name == "_map_reduce_with_thread_pool_and_buffers":
        experiment_name = "with_buffers"
    elif funct_name == "_map_reduce_with_thread_pool_no_feeding_queue":
        experiment_name = "no_feeding_queue"
    elif funct_name == "_map_reduce_with_thread_pool_with_feeding_queues":
        experiment_name = "with_feeding_queues"
    elif funct_name == "_map_reduce_naive":
        experiment_name = "naive"
    else:
        experiment_name = "other_experiment"

    for num_items_per_chunk in num_items_per_chunks:
        non_threaded_result = count_primes_standard(num_numbers_to_check)
        print(non_threaded_result)

        res = check_count_primes_performance(
            num_numbers_to_check,
            num_items_per_chunk,
            num_threads,
            num_feeding_queues,
            map_reduce_funct,
        )
        assert numpy.all(res["results"] == non_threaded_result["result"])
        print(res)

        base_fname = f"primes.{get_python_version()}.num_numbers_to_check_{num_numbers_to_check}.{experiment_name}"
        if experiment_name in ("with_buffers", "no_feeding_queue"):
            title = f"nums. checked: {num_numbers_to_check}, chunk_size: {num_items_per_chunk}"
            base_fname += f".num_items_per_chunk_{num_items_per_chunk}"
        elif experiment_name == "with_feeding_queues":
            title = f"nums. checked: {num_numbers_to_check}, chunk_size: {num_items_per_chunk}, n_feeding_queues: {num_feeding_queues}"
            base_fname += f".num_items_per_chunk_{num_items_per_chunk}.n_feeding_{num_feeding_queues}"
        elif experiment_name == "naive":
            title = f"nums. checked: {num_numbers_to_check}"
        else:
            title = f"nums. checked: {num_numbers_to_check}"

        plot_path = charts_dir / f"{base_fname}.time.png"

        fig, axes = plt.subplots()
        axes.plot(
            res["n_threads"],
            res["times_used"],
            linestyle="-",
            marker="o",
            color="blue",
        )
        axes.set_ylim(bottom=0, top=axes.get_ylim()[1])
        axes.set_ylabel("Time (s)")
        axes.set_xlabel("Num. threads")
        axes.set_title(title)
        fig.savefig(str(plot_path))

        speedup = res["times_used"][0] / res["times_used"]
        efficiency = speedup / res["n_threads"]
        plot_path = charts_dir / f"{base_fname}.efficiency.png"
        fig, axes = plt.subplots()
        axes.plot(
            res["n_threads"],
            efficiency,
            linestyle="-",
            marker="o",
            color="blue",
        )
        axes.set_ylim(bottom=0, top=axes.get_ylim()[1])
        axes.set_ylabel("Efficiency")
        axes.set_xlabel("Num. threads")
        axes.set_title(title)
        fig.savefig(str(plot_path))


def split_range(range_, num_items):
    start = range_.start
    stop = range_.stop
    for this_start in range(start, stop, num_items):
        this_stop = this_start + num_items
        if this_stop > stop:
            this_stop = stop
        yield range(this_start, this_stop)


def count_primes_in_range_standard(ranges_to_check):
    start_time = time()
    result = reduce(add, map(count_prime_numbers_in_range, ranges_to_check))
    end_time = time()
    return {"time_used": end_time - start_time, "result": result}


def check_count_primes_in_range_threaded(
    ranges_to_check,
    num_numbers_to_check,
    n_items_in_range,
    num_items_per_chunk,
    num_computing_threadss,
    num_feeding_queues,
):
    times = []
    results = []
    for num_computing_threads in num_computing_threadss:
        print("num threads", num_computing_threads)
        range_to_check = range(2, num_numbers_to_check)
        ranges_to_check = split_range(range_to_check, n_items_in_range)
        start_time = time()
        if num_feeding_queues:
            res = map_reduce_with_thread_pool_with_feeding_queues(
                count_prime_numbers_in_range,
                add,
                ranges_to_check,
                num_computing_threads=num_computing_threads,
                num_items_per_chunk=num_items_per_chunk,
                num_feeding_queues=num_feeding_queues,
            )
        else:
            res = map_reduce_with_thread_pool_no_feeding_queue(
                count_prime_numbers_in_range,
                add,
                ranges_to_check,
                num_computing_threads=num_computing_threads,
                num_items_per_chunk=num_items_per_chunk,
            )
        end_time = time()
        times.append(end_time - start_time)
        results.append(res)
        print(times)
    times = numpy.array(times)
    results = numpy.array(results)

    return {
        "times_used": times,
        "results": results,
        "n_threads": num_computing_threadss,
    }


def do_prime_range_experiment(
    n_items_in_range, num_items_per_chunks, num_threads, num_feeding_queues
):
    num_ranges_total = max(num_items_per_chunks) * 16 * 2
    num_numbers_to_check = num_ranges_total * n_items_in_range

    experiment_name = "primes_with_ranges"

    for num_items_per_chunk in num_items_per_chunks:
        range_to_check = range(2, num_numbers_to_check)
        ranges_to_check = split_range(range_to_check, n_items_in_range)
        non_threaded_result = count_primes_in_range_standard(ranges_to_check)

        res = check_count_primes_in_range_threaded(
            ranges_to_check,
            num_numbers_to_check,
            n_items_in_range,
            num_items_per_chunk,
            num_threads,
            num_feeding_queues,
        )
        assert numpy.all(res["results"] == non_threaded_result["result"])

        plot_path = (
            charts_dir
            / f"{experiment_name}.{get_python_version()}.n_numbers_to_check_{num_items_per_chunk}.n_items_in_range_{n_items_in_range}.n_items_per_chunk_{num_items_per_chunk}.n_feeding_queues.{num_feeding_queues}.time.png"
        )
        fig, axes = plt.subplots()
        axes.plot(
            res["n_threads"],
            res["times_used"],
            linestyle="-",
            marker="o",
            color="blue",
        )
        axes.set_ylim(bottom=0, top=axes.get_ylim()[1])
        axes.set_ylabel("Time (s)")
        axes.set_xlabel("Num. threads")
        axes.set_title(
            f"N. feeders: {num_feeding_queues}, N. items chunk: {num_items_per_chunk}, N. nums to check: {num_numbers_to_check}"
        )
        fig.savefig(str(plot_path))

        speedup = res["times_used"][0] / res["times_used"]
        efficiency = speedup / res["n_threads"]
        plot_path = (
            charts_dir
            / f"{experiment_name}.{get_python_version()}.n_numbers_to_check_{num_numbers_to_check}.n_items_in_range_{n_items_in_range}.n_items_per_chunk_{num_items_per_chunk}.n_feeding_queues.{num_feeding_queues}.efficiency.png"
        )
        fig, axes = plt.subplots()
        axes.plot(
            res["n_threads"],
            efficiency,
            linestyle="-",
            marker="o",
            color="blue",
        )
        axes.set_ylim(bottom=0, top=axes.get_ylim()[1])
        axes.set_ylabel("Efficiency")
        axes.set_xlabel("Num. threads")
        axes.set_title(
            f"N. feeders: {num_feeding_queues}, N. items chunk: {num_items_per_chunk}, N. nums to check: {num_numbers_to_check}"
        )
        fig.savefig(str(plot_path))


def get_python_version():
    version_info = sys.version_info
    version = f"{version_info.major}.{version_info.minor}.{version_info.micro}"
    if "free-threading" in sys.version:
        version += "t"
    return version


if __name__ == "__main__":
    performance_dir = Path(__file__).parent
    charts_dir = performance_dir / "charts"
    charts_dir.mkdir(exist_ok=True)
    map_reduce_funct = _map_reduce_with_thread_pool_and_buffers
    # map_reduce_funct = _map_reduce_with_thread_pool_no_feeding_queue
    map_reduce_funct = _map_reduce_naive

    if True:
        num_numbers_to_check = 1000000
        num_numbers_to_check = 100000
        num_items_per_chunks = (1000, 100, 1)
        num_threads = list(range(1, 17))
        num_feeding_queues = 0
        do_prime_experiment(
            num_numbers_to_check,
            num_items_per_chunks,
            num_threads,
            num_feeding_queues,
            map_reduce_funct,
        )

    if False:
        n_items_in_range = 10000
        num_items_per_chunks = (1, 20)
        num_threads = list(range(1, 17))
        num_feeding_queues = 1
        do_prime_range_experiment(
            n_items_in_range, num_items_per_chunks, num_threads, num_feeding_queues
        )
