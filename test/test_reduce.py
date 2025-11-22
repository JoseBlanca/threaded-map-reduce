from operator import add
from functools import reduce as funct_reduce

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
