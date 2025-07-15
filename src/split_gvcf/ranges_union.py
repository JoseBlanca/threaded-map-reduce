from genomicranges import GenomicRanges


def unify_two_ranges(ranges1: GenomicRanges, ranges2: GenomicRanges) -> GenomicRanges:
    return ranges1.union(ranges2)
