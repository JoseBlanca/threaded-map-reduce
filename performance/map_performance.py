from time import time
from statistics import mean

import numpy
from matplotlib.figure import Figure
from matplotlib.backends.backend_agg import FigureCanvasAgg as FigureCanvas
from matplotlib.ticker import MaxNLocator

from threaded_map_reduce.threaded_map_reduce import (
    _threaded_map_with_chunk_dispenser,
    _threaded_map_with_pool_executor,
    map_unordered,
)
from threaded_map_reduce import map as our_map
from performance_utils import is_prime, PERFORMANCE_CHARTS_DIR, BLUE, RED, GREY, GREEN


def do_non_threaded_experiment(
    num_threadss: list[int],
    chunk_sizes: list[int],
    mapping_fn,
    threaded_mapping_fn,
    items_to_map,
    num_threads_arg_name,
    num_repeats,
):
    times = []
    for _repeat_idx in range(num_repeats):
        time_start = time()
        non_threaded_result = list(map(mapping_fn, items_to_map))
        for _item_result in non_threaded_result:
            _item_result
        time_used = time() - time_start
        times.append(time_used)
    return {"time": mean(times)}


def do_threaded_experiment(
    num_threadss: list[int],
    chunk_sizes: list[int],
    mapping_fn,
    threaded_mapping_fn,
    items_to_map,
    num_threads_arg_name,
    num_repeats,
):
    results = []
    for chunk_size in chunk_sizes:
        times = []
        for num_threads in num_threadss:
            times_used = []
            for _ in range(num_repeats):
                time_start = time()
                kwagrs = {
                    "map_fn": mapping_fn,
                    "items": items_to_map,
                    "chunk_size": chunk_size,
                    num_threads_arg_name: num_threads,
                }
                threaded_result = threaded_mapping_fn(**kwagrs)

                for _item_result in threaded_result:
                    _item_result

                # print(threaded_result[:10])
                time_used = time() - time_start
                times_used.append(time_used)
            times.append(mean(times_used))
        result = {
            "chunk_size": chunk_size,
            "times": numpy.array(times),
            "num_threadss": num_threadss,
        }
        results.append(result)
    return results


def compare_with_futures_map_performance():
    num_numbers_to_check = 5000000
    # num_numbers_to_check = 50000
    chunk_sizes = (100,)
    num_threadss = list(range(1, 7))
    mapping_fn = is_prime
    num_repeats = 5

    experiment_futures_pool_map = {
        "num_threadss": num_threadss,
        "chunk_sizes": chunk_sizes,
        "mapping_fn": mapping_fn,
        "items_to_map": range(1, num_numbers_to_check),
        "num_threads_arg_name": "max_workers",
        "threaded_mapping_fn": _threaded_map_with_pool_executor,
        "num_repeats": num_repeats,
    }
    experiment_our_map = {
        "num_threadss": num_threadss,
        "chunk_sizes": chunk_sizes,
        "mapping_fn": mapping_fn,
        "items_to_map": range(1, num_numbers_to_check),
        "num_threads_arg_name": "num_computing_threads",
        "threaded_mapping_fn": our_map,
        "num_repeats": num_repeats,
    }
    experiment_map_unordered = {
        "num_threadss": num_threadss,
        "chunk_sizes": chunk_sizes,
        "mapping_fn": mapping_fn,
        "items_to_map": range(1, num_numbers_to_check),
        "num_threads_arg_name": "num_computing_threads",
        "threaded_mapping_fn": map_unordered,
        "num_repeats": num_repeats,
    }

    result_non_threaded = do_non_threaded_experiment(**experiment_futures_pool_map)
    non_threaded_time = result_non_threaded["time"]
    print(f"time non threaded: {non_threaded_time}")
    ideal_result = {
        "times": non_threaded_time / numpy.array(num_threadss),
        "num_threadss": num_threadss,
    }
    print(ideal_result)
    result_futures = do_threaded_experiment(**experiment_futures_pool_map)[0]
    print(result_futures)
    result_our_map = do_threaded_experiment(**experiment_our_map)[0]
    result_our_map_unordered = do_threaded_experiment(**experiment_map_unordered)[0]
    print(result_our_map)

    fig1 = Figure()
    _canvas1 = FigureCanvas(fig1)
    fig2 = Figure()
    _canvas2 = FigureCanvas(fig2)
    axes1 = fig1.add_subplot(1, 1, 1)
    axes2 = fig2.add_subplot(1, 1, 1)
    axes2.plot(
        ideal_result["num_threadss"],
        ideal_result["times"],
        linestyle="-",
        marker="o",
        color=GREY,
        label="ideal",
    )
    for axes in [axes1, axes2]:
        axes.plot(
            result_our_map["num_threadss"],
            result_our_map["times"],
            linestyle="-",
            marker="o",
            color=BLUE,
            label="this map",
        )
    axes1.plot(
        result_futures["num_threadss"],
        result_futures["times"],
        linestyle="-",
        marker="o",
        color=RED,
        label="futures pool map",
    )
    axes2.plot(
        result_our_map_unordered["num_threadss"],
        result_our_map_unordered["times"],
        linestyle="-",
        marker="o",
        color=GREEN,
        label="unordered map",
    )

    for axes in [axes1, axes2]:
        axes.legend()
        axes.set_ylabel("Time (s)")
        axes.set_xlabel("Num. threads")
        axes.xaxis.set_major_locator(MaxNLocator(integer=True))
        axes.set_ylim(0, axes.get_ylim()[1])

    PERFORMANCE_CHARTS_DIR.mkdir(exist_ok=True)
    plot_path = PERFORMANCE_CHARTS_DIR / "this_map_vs_futures_pool_executor_map.svg"
    fig1.savefig(plot_path, dpi=300)
    plot_path = PERFORMANCE_CHARTS_DIR / "this_map_vs_ideal.svg"
    fig2.savefig(plot_path, dpi=300)


def check_map_performance_with_primes():
    num_numbers_to_check = 3000000
    # num_numbers_to_check = 100000
    num_numbers_to_check = 50000
    chunk_sizes = (1000, 100, 1)
    chunk_sizes = (100,)
    num_threadss = list(range(1, 5))
    mapping_fn = is_prime

    charts_dir = PERFORMANCE_CHARTS_DIR
    charts_dir.mkdir(exist_ok=True)

    experiment1 = {
        "num_threadss": num_threadss,
        "chunk_sizes": chunk_sizes,
        "mapping_fn": mapping_fn,
        "items_to_map": range(1, num_numbers_to_check),
        "num_threads_arg_name": "max_workers",
        "threaded_mapping_fn": _threaded_map_with_pool_executor,
    }
    experiment2 = {
        "num_threadss": num_threadss,
        "chunk_sizes": chunk_sizes,
        "mapping_fn": mapping_fn,
        "items_to_map": range(1, num_numbers_to_check),
        "num_threads_arg_name": "num_computing_threads",
        "threaded_mapping_fn": _threaded_map_with_chunk_dispenser,
    }
    for experiment in [experiment1, experiment2]:
        result = do_non_threaded_experiment(**experiment)
        print(f"time non threaded: {result['time']}")
        results = do_threaded_experiment(**experiment)
        print(results)


if __name__ == "__main__":
    compare_with_futures_map_performance()
    # check_map_performance_with_primes()
