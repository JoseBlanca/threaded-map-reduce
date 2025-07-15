from io import BytesIO

import numpy

from split_gvcf.gvcf_parser import parse_gvcf_into_ranges
from split_gvcf.ranges_union import unify_two_ranges

VCF1 = b"""##fileformat=VCFv4.5
#CHROM\tPOS\tID\tREF\tALT\tQUAL\tFILTER\tINFO\tFORMAT\tNA00001\tNA00002\tNA00003
20\t9\trs6054257\tAG\tA,<NON_REF>\t29\tPASS\tNS=3;DP=14;AF=0.5;DB;H2\tGT:GQ:DP:HQ\t1|2:48:1:51,51\t3|4:48:8:51,51\t5/6000:43:5:.,.
20\t20\t.\tT\tT\t.\tq10\tNS=3;DP=11;AF=0.017\tGT:GQ:DP:HQ\t.|0:49:3:58,50\t0|1:3:5:65,3\t0/0:41:3
20\t30\t.\tA\tG,T\t67\tPASS\tNS=2;DP=10;AF=0.333,0.667;AA=T;DB\tGT:GQ:DP:HQ\t1|2:21:6:23,27\t2|1:2:0:18,2\t2/2:35:4
20\t40\t.\tT\t.\t47\tPASS\tNS=3;DP=13;AA=T\tGT:GQ:DP:HQ\t0|0:54:7:56,60\t0|0:48:4:51,51\t0/0:61:2
20\t50\tindel\tGTC\tG,GTCT\t50\tPASS\tNS=3;DP=9;AA=G\tGT:GQ:DP\t0/1:35:4\t0/2:17:2\t1/1:40:3
"""

VCF2 = b"""##fileformat=VCFv4.5
#CHROM\tPOS\tID\tREF\tALT\tQUAL\tFILTER\tINFO\tFORMAT\tNA00001\tNA00002\tNA00003
20\t10\trs6054257\tG\tA,<NON_REF>\t29\tPASS\tNS=3;DP=14;AF=0.5;DB;H2\tGT:GQ:DP:HQ\t1|2:48:1:51,51\t3|4:48:8:51,51\t5/6000:43:5:.,.
20\t20\t.\tT\t<NON_REF>\t.\tq10\tNS=3;DP=11;AF=0.017\tGT:GQ:DP:HQ\t.|0:49:3:58,50\t0|1:3:5:65,3\t0/0:41:3
20\t29\t.\tTA\tT,TAC,T\t67\tPASS\tNS=2;DP=10;AF=0.333,0.667;AA=T;DB\tGT:GQ:DP:HQ\t1|2:21:6:23,27\t2|1:2:0:18,2\t2/2:35:4
20\t40\t.\tT\t.\t47\tPASS\tNS=3;DP=13;AA=T\tGT:GQ:DP:HQ\t0|0:54:7:56,60\t0|0:48:4:51,51\t0/0:61:2
20\t50\tindel\tGTC\tG,GTCT\t50\tPASS\tNS=3;DP=9;AA=G\tGT:GQ:DP\t0/1:35:4\t0/2:17:2\t1/1:40:3
"""


def test_vcf_union():
    fhand1 = BytesIO(VCF1)
    fhand2 = BytesIO(VCF2)
    ranges1 = parse_gvcf_into_ranges(fhand1)
    ranges2 = parse_gvcf_into_ranges(fhand2)
    ranges = unify_two_ranges(ranges1, ranges2)
    assert numpy.all(ranges.start == [9, 20, 29, 40, 50])
    assert numpy.all(ranges.end == [10, 20, 31, 40, 53])
