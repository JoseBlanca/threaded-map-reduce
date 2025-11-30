# threaded-map-reduce

A Python library that implements **map**, **unordered map**, and **map-reduce** using threads.

This library is intented to be performant for CPU-bound tasks.
The map implementation has been tested to be much more performant than the map method of the [futures.ThreadPoolExecutor](https://docs.python.org/3/library/concurrent.futures.html#concurrent.futures.ThreadPoolExecutor) class of the standar library.

---

## Features

* **Parallel map** with deterministic order: `map`
* **Parallel unordered map** for maximum throughput: `map_unordered`
* **Parallel map-reduce**: `map_reduce`
* It groups items into chunks to reduce the parallelization overhead
* No external dependencies

---

## Installation

```bash
pip install threaded-map-reduce
```

(Or, if you use `uv`:)

```bash
uv pip install threaded-map-reduce
```

---

## Performance

### map-reduce

The `map_reduce` performance was tested with the following test:

- Task: Count the number of primes up to 5,000,000
- Workload: CPU-bound, using the same `is_prime` implementation for all tests (see the code at the end of this section)
- Chunk size for all parallel map implementations: 500
- Repetitions: 5 runs per configuration, reporting the mean runtime
- Python: free-threaded CPython 3.14.0
- Hardware: laptop with Intel® Core™ [i7-1260P](https://www.intel.com/content/www/us/en/products/sku/226254/intel-core-i71260p-processor-18m-cache-up-to-4-70-ghz/specifications.html) (4 performance cores, 6 efficiency cores)

![Threaded map-reduce performance](charts/thread_pool_and_buffers/reduce.3.14.0t.chunk_size_500.time.svg)

### map

To study the performance of the `map` function implemented in this library we ran a similar benchmark:

- Task: check whether each integer from 1 to 5,000,000 is prime
- Workload: CPU-bound, using the same `is_prime` implementation for all tests (see the code at the end of this section)
- Chunk size for all parallel map implementations: 500
- Repetitions: 5 runs per configuration, reporting the mean runtime
- Python: free-threaded CPython 3.14.0
- Hardware: laptop with Intel® Core™ [i7-1260P](https://www.intel.com/content/www/us/en/products/sku/226254/intel-core-i71260p-processor-18m-cache-up-to-4-70-ghz/specifications.html) (4 performance cores, 6 efficiency cores)

![ideal vs this map performance](charts/this_map_vs_ideal.3.14.0t.chunk_size_500.svg)

The performance of this library’s map implementation improves with the number of threads.
For this task there is no appreciable difference between the ordered and unordered maps.

Compared with the ideal scaling, for two and four threads our implementation is 1.19 and 1.54 times slower respectively.
In an ideal scenario, the runtime with N threads to perform the task should be the time with just one thread (using the standard non-threaded [map](https://docs.python.org/3/library/functions.html#map) implementation) divided by N.

### Chunk size

The chunk size is a critical parameter for performance.
Both map and map_reduce process items in chunks, and the parallelization overhead depends strongly on how many items are grouped into each chunk.

Using very small chunks (e.g. one item per chunk) usually produces poor performance, because the cost of thread scheduling and queue operations dominates the useful work.
Using very large chunks eventually stops giving additional speedups, and may increase memory usage.

We used the same banchmark as in the previous map section. A chunk size of 1 is 6.7× slower than a chunk size of 500, and increasing the chunk size beyond 500 does not reduce the runtime further.
The optimal chunk size depends on the specific workload.

Note that each chunk is materialized as a list before being processed, so larger chunk sizes increase memory usage.

Time used to carry out the task for different chunk sizes:

![Time used to do the task for different chunk sizes](charts/chunk_size_relevance.3.14.0t.svg)

### ThreadPoolExecutor.map vs this library’s `map`

We compared, using the same mapping benchmark, the performance of this library's map implementation with the one available in the [ThreadPoolExecutor](https://docs.python.org/3/library/concurrent.futures.html#concurrent.futures.ThreadPoolExecutor) found in the [futures module](https://docs.python.org/3/library/concurrent.futures.html) of the standard library: 

![futures map vs this map performance](charts/this_map_vs_futures_pool_executor_map.3.14.0t.chunk_size_500.svg)

In this benchmark, the `ThreadPoolExecutor.map` implementation is 4.11 (1 thread) times slower than the `threaded_map` implementation when using a single
thread, and its performance degrades further as the number of threads
increases (e.g. 5.99 slower with (4 threads)).


### is_prime function

The is_prime function used was:

```{python}
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
    for f in range(5, r + 1, 6):
        if n % f == 0:
            return False
        elif n % (f + 2) == 0:
            return False
    return True
```

## Quick Start

### 1. Parallel map (ordered)

```python
from threaded_map_reduce import map

def square(x):
    return x * x

nums = range(1000)
result = list(threaded_map(square, nums, num_computing_threads=4, chunk_size=100))
print(result[-10:])
```

---

### 2. Parallel map (unordered)

Faster, but order is not preserved:

```python
from threaded_map_reduce import map_unordered

nums = range(1000)
result = list(map_unordered(square, nums, num_computing_threads=4, chunk_size=100))
print(sorted(result))
print(result[-10:])
```

---

### 3. Parallel map-reduce

Useful for reductions such as sums, counts, or any associative operation.

```python
from operator import add
from threaded_map_reduce import map_reduce

def square(x):
    return x * x

nums = range(0, 1000)
result = map_reduce(square, add, nums,
                    num_computing_threads=4,
                    chunk_size=100)
print(result)
```

---

## API Summary

### `threaded_map(map_fn, items, num_computing_threads, chunk_size)`

Runs `map_fn` over every item in parallel and yields results keeping input order.

### `map_unordered(map_fn, items, num_computing_threads, chunk_size)`

Same as above, but yields items in any order.

### `map_reduce(map_fn, reduce_fn, items, num_computing_threads, chunk_size)`

Maps items in parallel, reduces mapped chunks using the provided reducer function, and returns a single result.

---

## License

MIT License.
