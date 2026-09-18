from operator import add
from functools import reduce as funct_reduce
import time

from threaded_map_reduce import map_reduce, map_unordered
from threaded_map_reduce import map as threaded_map


def square(a):
    return a**2


def test_map_reduce():
    num_items = 101
    nums = range(num_items)
    squares = map(square, nums)
    result1 = funct_reduce(add, squares)
    nums = range(num_items)
    result2 = map_reduce(square, add, nums, num_computing_threads=4, chunk_size=10)
    assert result1 == result2


def test_map():
    num_items = 100
    nums = range(num_items)
    squares = list(map(square, nums))
    squares2 = threaded_map(square, nums, num_computing_threads=4, chunk_size=5)
    assert squares == list(squares2)

    nums = range(num_items)
    squares2 = map_unordered(square, nums, num_computing_threads=4, chunk_size=5)
    assert sorted(squares) == sorted(squares2)


def slow_numbers(num_items):
    """An iterable slow enough for the work to reach more than one thread.

    With an iterable that is instant to go through, one single thread tends to
    take every chunk before the others get going, so the bugs that only show up
    when the chunks are spread over the threads stay hidden.
    """
    for num in range(num_items):
        time.sleep(0.0005)
        yield num


def test_map_keeps_every_item_when_the_work_is_spread():
    num_items = 300
    expected = list(map(square, range(num_items)))

    for num_computing_threads in (1, 2, 4):
        mapped = threaded_map(
            square,
            slow_numbers(num_items),
            num_computing_threads=num_computing_threads,
            chunk_size=50,
        )
        assert list(mapped) == expected


def test_map_unordered_keeps_every_item_when_the_work_is_spread():
    num_items = 300
    expected = sorted(map(square, range(num_items)))
    mapped = map_unordered(
        square, slow_numbers(num_items), num_computing_threads=4, chunk_size=50
    )
    assert sorted(mapped) == expected


def test_map_reduce_keeps_every_item_when_the_work_is_spread():
    num_items = 300
    expected = funct_reduce(add, map(square, range(num_items)))
    result = map_reduce(
        square, add, slow_numbers(num_items), num_computing_threads=4, chunk_size=50
    )
    assert result == expected
