from math import sqrt
from pathlib import Path
import sys

PERFORMANCE_CHARTS_DIR = Path(__file__).parent.parent / "charts"
BLUE = "tab:blue"
RED = "tab:red"
GREEN = "tab:green"
GREY = "tab:gray"


def get_python_version():
    version_info = sys.version_info
    version = f"{version_info.major}.{version_info.minor}.{version_info.micro}"
    if "free-threading" in sys.version:
        version += "t"
    return version


def is_prime(n):
    if n == 1:
        return False
    elif n == 2 or n == 3:
        return True
    elif n % 2 == 0:
        return False
    elif n < 9:
        return True
    elif n % 3 == 0:
        return False
    r = int(sqrt(n))
    # since all primes > 3 are of the form 6n ± 1
    # start with f=5 (which is prime)
    # and test f, f+2 for being prime
    # then loop by 6.
    for f in range(5, r + 1, 6):
        if n % f == 0:
            return False
        elif n % (f + 2) == 0:
            return False
    return True


def test_is_prime():
    primes = [n for n in range(1, 200) if is_prime(n)]
    assert primes == [
        2,
        3,
        5,
        7,
        11,
        13,
        17,
        19,
        23,
        29,
        31,
        37,
        41,
        43,
        47,
        53,
        59,
        61,
        67,
        71,
        73,
        79,
        83,
        89,
        97,
        101,
        103,
        107,
        109,
        113,
        127,
        131,
        137,
        139,
        149,
        151,
        157,
        163,
        167,
        173,
        179,
        181,
        191,
        193,
        197,
        199,
    ]


if __name__ == "__main__":
    test_is_prime()
