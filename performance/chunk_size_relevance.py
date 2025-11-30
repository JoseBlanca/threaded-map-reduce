from matplotlib.figure import Figure
from matplotlib.backends.backend_agg import FigureCanvasAgg as FigureCanvas

from futures_vs_this_map_performance import do_threaded_experiment
from threaded_map_reduce import map as our_map
from performance_utils import (
    is_prime,
    PERFORMANCE_CHARTS_DIR,
    BLUE,
    get_python_version,
)


def check_chunk_size_relevance():
    num_numbers_to_check = 5000000
    # num_numbers_to_check = 100000
    chunk_sizes = [1, 10, 20, 30, 40, 50, 100, 200, 300, 500, 750, 1000, 2000]
    num_threads = 4
    mapping_fn = is_prime
    num_repeats = 5

    experiment_our_map = {
        "num_threadss": (num_threads,),
        "chunk_sizes": chunk_sizes,
        "mapping_fn": mapping_fn,
        "items_to_map": range(1, num_numbers_to_check),
        "num_threads_arg_name": "num_computing_threads",
        "threaded_mapping_fn": our_map,
        "num_repeats": num_repeats,
    }
    results = do_threaded_experiment(**experiment_our_map)
    chunk_sizes = []
    times = []
    for result in results:
        chunk_sizes.append(result["chunk_size"])
        times.append(float(result["times"][0]))
    fig = Figure()
    _canvas = FigureCanvas(fig)
    axes = fig.add_subplot(1, 1, 1)
    axes.plot(
        chunk_sizes,
        times,
        linestyle="-",
        marker="o",
        color=BLUE,
        label="ideal",
    )

    axes.set_xlabel("Chunk size")
    axes.set_ylabel("Time (s)")
    axes.set_yscale("log")

    PERFORMANCE_CHARTS_DIR.mkdir(exist_ok=True)
    plot_path = (
        PERFORMANCE_CHARTS_DIR / f"chunk_size_relevance.{get_python_version()}.svg"
    )
    fig.savefig(plot_path)


if __name__ == "__main__":
    check_chunk_size_relevance()
