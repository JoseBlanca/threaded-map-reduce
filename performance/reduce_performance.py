from operator import add
from functools import reduce, partial
from time import time, sleep
from pathlib import Path
import sys

import matplotlib.pyplot as plt
import numpy

from threaded_map_reduce.threaded_map_reduce import map_reduce


def wait_sleep(_, seconds):
    sleep(seconds)
    return seconds


def wait_cpu(_, seconds):
    start_time = time()
    while True:
        elapsed = time() - start_time
        if elapsed >= seconds:
            break
    return elapsed


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


def count_primes_standard(num_numbers):
    numbers = range(1, num_numbers)
    start_time = time()

    total = reduce(add, map(is_prime, numbers))

    time_used = time() - start_time
    return {"time_used": time_used, "result": total}


def count_primes_threaded(num_numbers, num_computing_threads, num_items_per_chunk):
    numbers = range(1, num_numbers)
    start_time = time()

    total = map_reduce(
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


def check_count_primes_performance():
    numbers_to_check = 4000000
    res = count_primes_standard(numbers_to_check)
    print("standard: ", res["time_used"], res["result"])
    for num_items in range(10, 110, 10):
        res = count_primes_threaded(numbers_to_check, 4, num_items)
        print(f"threaded, num_items ({num_items}): ", res["time_used"], res["result"])

    for num_threads in range(1, 17):
        res = count_primes_threaded(numbers_to_check, num_threads, 1000)
        print(
            f"threaded, num_threads ({num_threads}): ", res["time_used"], res["result"]
        )


def do_sleeping_standard(seconds_to_sleep, number_of_sleeps, function):
    do_waiting = partial(function, seconds=seconds_to_sleep)
    start_time = time()
    res = map(do_waiting, range(number_of_sleeps))
    res = reduce(add, res)
    end_time = time()
    return {"time_used": end_time - start_time}


def do_sleeping_threaded(
    seconds_to_sleep,
    number_of_sleeps,
    num_computing_threads,
    num_items_per_chunk,
    function,
):
    do_waiting = partial(function, seconds=seconds_to_sleep)
    start_time = time()
    res = map_reduce(
        do_waiting,
        add,
        range(number_of_sleeps),
        num_computing_threads=num_computing_threads,
        num_items_per_chunk=num_items_per_chunk,
    )
    end_time = time()
    return {"time_used": end_time - start_time, "result": res}


def check_sleeping_performance(
    num_seconds_sleeping_per_step, total_num_seconds_sleeping, function
):
    num_threads = []
    time_used = []
    results = []
    for n_threads in range(1, 17):
        num_seconds_sleeping_per_thread = total_num_seconds_sleeping / n_threads
        number_of_sleeps_in_thread = round(
            num_seconds_sleeping_per_thread / num_seconds_sleeping_per_step
        )
        if not number_of_sleeps_in_thread:
            number_of_sleeps_in_thread = 1
        actual_num_seconds_sleeping_per_step = total_num_seconds_sleeping / (
            number_of_sleeps_in_thread * n_threads
        )
        if not number_of_sleeps_in_thread:
            number_of_sleeps_in_thread = 1
        res = do_sleeping_threaded(
            actual_num_seconds_sleeping_per_step,
            number_of_sleeps_in_thread * n_threads,
            num_computing_threads=n_threads,
            num_items_per_chunk=1,
            function=function,
        )
        num_threads.append(n_threads)
        time_used.append(res["time_used"])
        results.append(res["result"])
    return {
        "n_threads": numpy.array(num_threads),
        "time_used": numpy.array(time_used),
        "results": numpy.array(results),
    }


def do_sleep_experiment(function):
    total_num_seconds_sleepings = (1, 20, 10, 1)
    num_seconds_sleeping_per_steps = (0.1, 1, 0.1, 0.00001)
    for num_seconds_sleeping_per_step, total_num_seconds_sleeping in zip(
        num_seconds_sleeping_per_steps, total_num_seconds_sleepings
    ):
        res1 = check_sleeping_performance(
            num_seconds_sleeping_per_step=num_seconds_sleeping_per_step,
            total_num_seconds_sleeping=total_num_seconds_sleeping,
            function=function,
        )
        print(res1)
        res2 = do_sleeping_standard(
            total_num_seconds_sleeping, number_of_sleeps=1, function=function
        )
        print(res2)
        plot_path = (
            charts_dir
            / f"slepping.{function.__name__}.{get_python_version()}.time.num_seconds_per_step_{num_seconds_sleeping_per_step}.total_seconds_{total_num_seconds_sleeping}.png"
        )
        fig, axes = plt.subplots()
        axes.plot(
            res1["n_threads"],
            res1["time_used"],
            linestyle="-",
            marker="o",
            color="blue",
        )
        axes.set_ylim(bottom=0, top=axes.get_ylim()[1])
        axes.set_ylabel("Time (s)")
        axes.set_xlabel("Num. threads")

        axes.hlines(
            res2["time_used"],
            xmin=min(res1["n_threads"]),
            xmax=max(res1["n_threads"]),
            color="red",
        )
        fig.savefig(str(plot_path))
        plot_path = (
            charts_dir
            / f"slepping.{function.__name__}.{get_python_version()}.efficiency.num_seconds_per_step_{num_seconds_sleeping_per_step}.total_seconds_{total_num_seconds_sleeping}.png"
        )
        speedup = res1["time_used"][0] / res1["time_used"]
        efficiency = speedup / res1["n_threads"]
        fig, axes = plt.subplots()
        axes.plot(
            res1["n_threads"],
            efficiency,
            linestyle="-",
            marker="o",
            color="blue",
        )
        axes.set_ylim(bottom=0, top=axes.get_ylim()[1])
        axes.set_ylabel("Efficiency")
        axes.set_xlabel("Num. threads")

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

    python = get_python_version()

    do_sleep_experiment(function=wait_cpu)
    do_sleep_experiment(function=wait_sleep)

    # check_add_numbers_performance()

    # check_count_primes_performance()
