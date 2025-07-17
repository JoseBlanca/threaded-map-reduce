from operator import add
from functools import reduce, partial
from time import time, sleep

from threaded_map_reduce.threaded_map_reduce import map_reduce


def wait(_, seconds):
    print("task started", seconds)
    start_time = time()
    while True:
        if time() - start_time >= seconds:
            break
    return 1


def square(num):
    return num**2


def add_numbers_standard(num_numbers_to_add):
    numbers = range(num_numbers_to_add)
    start_time = time()
    squares = map(square, numbers)
    total = reduce(add, squares)
    time_used = time() - start_time
    return {"time_used": time_used, "result": total}


def add_numbers_threaded(num_numbers_to_add, max_workers):
    numbers = range(num_numbers_to_add)
    start_time = time()
    total = map_reduce(square, add, numbers, max_workers=max_workers)
    time_used = time() - start_time
    return {"time_used": time_used, "result": total}


def check_add_numbers_performance():
    numbers_to_add = 2000000
    res = add_numbers_standard(numbers_to_add)
    print("standard: ", res["time_used"], res["result"])
    res = add_numbers_threaded(numbers_to_add, 4)
    print("threaded: ", res["time_used"], res["result"])


def do_sleeping_standard(seconds_to_sleep, number_of_sleeps):
    do_waiting = partial(wait, seconds=seconds_to_sleep)
    start_time = time()
    res = map(do_waiting, range(number_of_sleeps))
    res = reduce(add, res)
    end_time = time()
    return {"time_used": end_time - start_time, "result": res}


def do_sleeping_threaded(seconds_to_sleep, number_of_sleeps, max_workers):
    do_waiting = partial(wait, seconds=seconds_to_sleep)
    start_time = time()
    res = map_reduce(do_waiting, add, range(number_of_sleeps), max_workers=max_workers)
    end_time = time()
    return {"time_used": end_time - start_time, "result": res}


def check_sleeping_performance():
    seconds_to_sleep = 10
    number_of_sleeps = 10
    # res = do_sleeping_standard(seconds_to_sleep, number_of_sleeps)
    # print("standard: ", res["time_used"])
    res = do_sleeping_threaded(seconds_to_sleep, number_of_sleeps, max_workers=4)
    print("threaded: ", res["time_used"])


if __name__ == "__main__":
    check_add_numbers_performance()
    # check_sleeping_performance()
