from typing import Iterable
from functools import reduce
from concurrent.futures import ThreadPoolExecutor

from genomicranges import GenomicRanges


def unify_two_ranges(
    ranges1: GenomicRanges | None, ranges2: GenomicRanges
) -> GenomicRanges:
    if ranges1 is None:
        return ranges2
    else:
        return ranges1.union(ranges2)
