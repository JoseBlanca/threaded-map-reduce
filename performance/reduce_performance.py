from operator import add
from functools import reduce, partial
from time import time, sleep

from threaded_map_reduce.threaded_map_reduce import map_reduce


def wait(_, seconds):
    sleep(seconds)
    return 1


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


def do_sleeping_standard(seconds_to_sleep, number_of_sleeps):
    do_waiting = partial(wait, seconds=seconds_to_sleep)
    start_time = time()
    res = map(do_waiting, range(number_of_sleeps))
    res = reduce(add, res)
    end_time = time()
    return {"time_used": end_time - start_time, "result": res}


def do_sleeping_threaded(
    seconds_to_sleep, number_of_sleeps, num_computing_threads, num_items_per_chunk
):
    do_waiting = partial(wait, seconds=seconds_to_sleep)
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


def check_sleeping_performance():
    for seconds_to_sleep in (0.00001, 0.1, 1):
        print("seconds_to_sleep", seconds_to_sleep)
        number_of_sleeps = int(10 / seconds_to_sleep)

        # res = do_sleeping_standard(seconds_to_sleep, number_of_sleeps)
        # print("standard: ", res["time_used"])
        number_of_sleeps = 2
        for n_threads in range(1, 17):
            res = do_sleeping_threaded(
                seconds_to_sleep,
                number_of_sleeps,
                num_computing_threads=n_threads,
                num_items_per_chunk=1,
            )
            print(f"threaded, n_threads ({n_threads}): ", res["time_used"])


if __name__ == "__main__":
    # check_add_numbers_performance()
    check_sleeping_performance()
    # check_count_primes_performance()
