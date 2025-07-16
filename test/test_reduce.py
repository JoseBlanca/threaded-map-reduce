from operator import add
from functools import reduce as funct_reduce

from split_gvcf.threaded_reduce import reduce


def test_reduce():
    nums = range(101)
    result1 = funct_reduce(add, nums)
    nums = range(101)
    result2 = reduce(add, nums, max_workers=4)
    assert result1 == result2
