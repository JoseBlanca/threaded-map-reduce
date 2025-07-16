from operator import add
from functools import reduce as funct_reduce

from split_gvcf.threaded_reduce import reduce


def test_reduce():
    num_items_gauss_added = 101
    nums = range(num_items_gauss_added)
    result1 = funct_reduce(add, nums)
    nums = range(num_items_gauss_added)
    result2 = reduce(add, nums, max_workers=4)
    assert result1 == result2
