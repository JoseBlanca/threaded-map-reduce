from operator import add
from functools import reduce as funct_reduce

from threaded_map_reduce import map_reduce, threaded_map


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
    num_items = 101
    nums = range(num_items)
    squares = map(square, nums)
    squares2 = threaded_map(square, nums, num_computing_threads=2)
    assert list(squares) == list(squares2)
