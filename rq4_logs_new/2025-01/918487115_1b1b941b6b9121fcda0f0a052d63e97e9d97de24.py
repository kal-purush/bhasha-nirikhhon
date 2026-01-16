import os
import time

import pandas as pd
from utils.cluster_utils import generate_clusters_parallel
from utils.dd_utils import extract_attributes_thresholds, find_violating_pairs_parallel
from utils.difference import generate_difference_file
from utils.distance_utils import build_all_distance_lookups
from utils.evaluation_utils import (
    calculate_precision_recall_f1,
    check_all_pairs_covered,
)

# Import your main modules
from utils.MWVC import build_graph_parallel
from utils.preprocessing import generate_probability_files

if __name__ == "__main__":
    # ------------------------------------------------------------------
    # 1) File paths for your single dataset
    # ------------------------------------------------------------------
    dirty_file = r"path\data\Flight\dirty.csv"
    clean_file = r"path\data\Flight\clean.csv"
    dd_constraints_file = r"path\data\Flight\dd_constraints.txt"
    attribute_thresholds_file = r"path\data\Flight\attribute_thresholds.txt"

    # Output files that we will generate
    difference_file = r"path\data\Flight\difference.csv"
    probability_file = r"path\data\Flight\predicted_probabilities.csv"

    # ------------------------------------------------------------------
    # 2) Read dd_constraints and attribute_thresholds
    # ------------------------------------------------------------------
    with open(dd_constraints_file, "r", encoding="utf-8") as f:
        dd_constraints = eval(f.read())

    with open(attribute_thresholds_file, "r", encoding="utf-8") as f:
        attribute_thresholds = eval(f.read())

    # ------------------------------------------------------------------
    # 3) Number of processes
    # ------------------------------------------------------------------
    FIXED_NUM_PROCESSES = 16

    # ------------------------------------------------------------------
    # 4) Extract thresholds from dd_constraints
    # ------------------------------------------------------------------
    start_time = time.time()
    sorted_thresholds = extract_attributes_thresholds(dd_constraints)
    print("Time for extracting thresholds:", time.time() - start_time)

    # ------------------------------------------------------------------
    # 5) Generate difference file (dirty vs. clean) for ground truth
    # ------------------------------------------------------------------
    print("Generating difference file...")
    generate_difference_file(dirty_file, clean_file, difference_file)

    # Read difference to build ground_truth_errors
    diff_df = pd.read_csv(difference_file)
    ground_truth_errors = set(
        f"t{row['Index']}.{row['Attribute']}" for _, row in diff_df.iterrows()
    )
    print(f"Number of ground_truth_errors: {len(ground_truth_errors)}")

    # ------------------------------------------------------------------
    # 6) Generate probability file from the dirty dataset
    # ------------------------------------------------------------------
    print("Generating probability file...")
    # 1) Derive dataset scheme from columns of the dirty CSV
    dirty_df = pd.read_csv(dirty_file, dtype=str)
    dataset_scheme = list(dirty_df.columns)

    # 2) Generate probabilities
    generate_probability_files(dirty_file, probability_file, dataset_scheme)

    # ------------------------------------------------------------------
    # 7) Convert the dirty dataset to dict records for further steps
    # ------------------------------------------------------------------
    data_instance = dirty_df.to_dict(orient="records")
    length = len(data_instance)
    print(f"Dataset length: {length}")

    # ------------------------------------------------------------------
    # 8) Build all distance lookups
    # ------------------------------------------------------------------
    start_time_lookup = time.time()
    distance_lookups = build_all_distance_lookups(
        data_instance, sorted_thresholds, FIXED_NUM_PROCESSES
    )
    end_time_lookup = time.time()
    build_lookup_time = end_time_lookup - start_time_lookup
    print(f"Time for building distance lookups: {build_lookup_time:.4f} seconds")

    # ------------------------------------------------------------------
    # 9) Generate clusters
    # ------------------------------------------------------------------
    start_time_cluster = time.time()
    clusters = generate_clusters_parallel(
        data_instance, sorted_thresholds, distance_lookups, FIXED_NUM_PROCESSES
    )
    end_time_cluster = time.time()
    cluster_time = end_time_cluster - start_time_cluster
    print(f"Time for generating clusters: {cluster_time:.4f} seconds")

    # ------------------------------------------------------------------
    # 10) Find violating pairs
    # ------------------------------------------------------------------
    start_time_find = time.time()
    find_violating_pair, hyper_edges = find_violating_pairs_parallel(
        dd_constraints, clusters, FIXED_NUM_PROCESSES, length, delta_n=0
    )
    end_time_find = time.time()
    find_vio_pairs_time = end_time_find - start_time_find
    print(f"Time for finding violating pairs: {find_vio_pairs_time:.4f} seconds")

    # ------------------------------------------------------------------
    # 11) Build graph (EDet)
    # ------------------------------------------------------------------
    start_time_edet = time.time()
    E_final = build_graph_parallel(
        hyper_edges,
        probability_file,
        FIXED_NUM_PROCESSES,
        attribute_thresholds=attribute_thresholds,
    )
    end_time_edet = time.time()
    edet_time = end_time_edet - start_time_edet
    print(f"Time for EDet: {edet_time:.4f} seconds")

    # ------------------------------------------------------------------
    # 12) Calculate total time
    # ------------------------------------------------------------------
    total_time = build_lookup_time + cluster_time + find_vio_pairs_time + edet_time
    print(f"Total time for the pipeline: {total_time:.4f} seconds")

    # ------------------------------------------------------------------
    # 13) Evaluate results
    # ------------------------------------------------------------------
    precision, recall, f1_score_val = calculate_precision_recall_f1(
        E_final, ground_truth_errors
    )
    coverage_msg = check_all_pairs_covered(find_violating_pair, E_final)
    print(
        f"Metrics => Precision={precision:.4f}, Recall={recall:.4f}, F1={f1_score_val:.4f}"
    )
    print("Coverage check:", coverage_msg)

    # ------------------------------------------------------------------
    # 14) Store results in a DataFrame (single row)
    # ------------------------------------------------------------------
    results = {
        "Precision": precision,
        "Recall": recall,
        "F1": f1_score_val,
        "BuildLookupTime": build_lookup_time,
        "ClusterTime": cluster_time,
        "FindVioPairsTime": find_vio_pairs_time,
        "EDetTime": edet_time,
        "TotalTime": total_time,
        "Coverage": coverage_msg,
    }

    df_results = pd.DataFrame([results])
    print("\n===== Single Dataset Experiment Results =====")
    print(df_results)