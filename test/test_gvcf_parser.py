from io import BytesIO

import numpy

from split_gvcf.gvcf_parser import parse_gvcf, parse_gvcf_into_ranges

VCF = b"""##fileformat=VCFv4.5
#CHROM\tPOS\tID\tREF\tALT\tQUAL\tFILTER\tINFO\tFORMAT\tNA00001\tNA00002\tNA00003
20\t10\trs6054257\tG\tA,<NON_REF>\t29\tPASS\tNS=3;DP=14;AF=0.5;DB;H2\tGT:GQ:DP:HQ\t1|2:48:1:51,51\t3|4:48:8:51,51\t5/6000:43:5:.,.
20\t20\t.\tT\t<NON_REF>\t.\tq10\tNS=3;DP=11;AF=0.017\tGT:GQ:DP:HQ\t.|0:49:3:58,50\t0|1:3:5:65,3\t0/0:41:3
20\t30\t.\tA\tG,T\t67\tPASS\tNS=2;DP=10;AF=0.333,0.667;AA=T;DB\tGT:GQ:DP:HQ\t1|2:21:6:23,27\t2|1:2:0:18,2\t2/2:35:4
20\t40\t.\tT\t.\t47\tPASS\tNS=3;DP=13;AA=T\tGT:GQ:DP:HQ\t0|0:54:7:56,60\t0|0:48:4:51,51\t0/0:61:2
20\t50\tindel\tGTC\tG,GTCT\t50\tPASS\tNS=3;DP=9;AA=G\tGT:GQ:DP\t0/1:35:4\t0/2:17:2\t1/1:40:3
"""


def test_parse_gvcf():
    fhand = BytesIO(VCF)
    expected = [("20", 10, 1), ("20", 30, 1), ("20", 40, 1), ("20", 50, 4)]
    for idx, var in enumerate(parse_gvcf(fhand)):
        assert expected[idx] == var


def test_parse_into_ranges():
    fhand = BytesIO(VCF)
    ranges = parse_gvcf_into_ranges(fhand)
    assert numpy.all(ranges.start == [10, 30, 40, 50])
