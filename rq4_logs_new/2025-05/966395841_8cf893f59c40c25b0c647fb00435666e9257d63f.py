#!/usr/bin/env python3
import astar_metrics_ext

def run_search(config):
    """
    Runs A★ with the given config dict through the pybind11 extension,
    then computes prediction and coverage metrics.
    """

    map_path   = config["map"]
    start      = tuple(map(int, config["start"].split(',')))
    goal       = tuple(map(int, config["goal"].split(',')))
    radius     = config["radius"]
    threads    = config["threads"]
    algo       = config["algorithm"]
    max_depth  = config["max_depth"]

    #  Run the search
    path, elapsed_ms, mets = astar_metrics_ext.run_astar(
        map_path,
        start,
        goal,
        radius,
        threads,
        max_depth,
        algo,
        True           # enable_metrics
    )

    #  Extract raw counters
    expansions             = mets["expansions"]
    demand_checks          = mets["demand_checks"]
    total_speculations     = mets["total_speculations"]
    successful_speculations= mets["successful_speculations"]

    #  Compute prediction & coverage
    prediction = successful_speculations/ total_speculations if total_speculations > 0 else 0.0
    if total_speculations + demand_checks > 0:
        coverage = total_speculations / (total_speculations + demand_checks)
    else:
        coverage = 0.0

    #  Report
    print(f"Algorithm        : {algo}")
    print(f"Map              : {map_path}")
    print(f"Start -> Goal     : {start} -> {goal}")
    print(f"Radius           : {radius}")
    print(f"Threads          : {threads}")
    print(f"Max depth        : {max_depth}")
    print()
    print(f"Path length      : {len(path)} nodes")
    print(f"Time             : {elapsed_ms:.2f} ms")
    print()
    print("Metrics:")
    print(f"  expansions               : {expansions}")
    print(f"  demand_checks            : {demand_checks}")
    print(f"  total_speculations       : {total_speculations}")
    print(f"  successful_speculations  : {successful_speculations}")
    print()
    print(f"Prediction (used specs)   : {prediction:.2%}")
    print(f"Coverage (specs/total)    : {coverage:.2%}")



if __name__ == "__main__":
    # your provided config
    config = {
        "map"         : "./dataset/boston/Boston_0_1024.map",
        "start"       : "332,758",
        "goal"        : "80,773",
        "radius"      : 5,
        "threads"     : 1,
        "algorithm"   : "serial",  #
        "max_depth"   : 2
    }
    run_search(config)