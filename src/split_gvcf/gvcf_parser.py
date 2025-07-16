from enum import Enum
from typing import Iterator, Tuple, BinaryIO
from array import array
from concurrent.futures import ThreadPoolExecutor

import iranges
from genomicranges import GenomicRanges

FILTER_FIELD_IDX = 5
NON_REF = b"<NON_REF>"


class VcfSection(Enum):
    HEADER = 1
    BODY = 2


def parse_gvcf(fhand) -> Iterator[Tuple[str, int, int]]:
    section = VcfSection.HEADER
    for line in fhand:
        if section == VcfSection.HEADER:
            if line.startswith(b"##"):
                continue
            elif line.startswith(b"#CHROM"):
                section = VcfSection.BODY
                continue
            else:
                raise RuntimeError(
                    "In section body lines that not start with # are not allowed"
                )

        chrom, pos, _, ref, alt, _ = line.split(b"\t", FILTER_FIELD_IDX)
        chrom = chrom.decode()
        pos = int(pos)
        if alt == NON_REF:
            continue
        alt_alleles = filter(lambda x: x != NON_REF, alt.split(b","))
        max_alt_allele_len = max(map(len, alt_alleles))
        max_allele_len = max((len(ref), max_alt_allele_len))
        yield chrom, pos, max_allele_len


def parse_gvcf_into_ranges(fhand) -> GenomicRanges:
    seq_names = []
    starts = array("L")
    widths = array("L")
    for var in parse_gvcf(fhand):
        seq_names.append(var[0])
        _, pos, max_allele_len = var
        starts.append(pos)
        widths.append(max_allele_len)
    ranges = iranges.IRanges(starts, widths)
    ranges = GenomicRanges(seqnames=seq_names, ranges=ranges)
    return ranges
