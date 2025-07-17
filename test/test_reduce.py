from operator import add
from functools import reduce as funct_reduce

from threaded_map_reduce.threaded_map_reduce import map_reduce, reduce


def test_reduce():
    num_items_gauss_added = 101
    nums = range(num_items_gauss_added)
    result1 = funct_reduce(add, nums)
    nums = range(num_items_gauss_added)
    result2 = reduce(add, nums, max_workers=4)
    assert result1 == result2


def square(a):
    return a**2


def test_map_reduce():
    num_items = 101
    nums = range(num_items)
    squares = map(square, nums)
    result1 = funct_reduce(add, squares)
    nums = range(num_items)
    result2 = map_reduce(square, add, nums, max_workers=4)
    assert result1 == result2
