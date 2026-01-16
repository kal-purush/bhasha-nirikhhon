from diffprivlib.mechanisms import Laplace
import numpy as np
from pathlib import Path

def apply_ldp_to_sparse_vector(vector, epsilon, lower_bound=0, upper_bound=None):
    """
    Apply Local Differential Privacy (LDP) to a sparse vector.

    Parameters:
    - vector: The sparse vector (numpy array).
    - epsilon: Privacy budget for LDP.
    - lower_bound: Minimum possible value in the data.
    - upper_bound: Optional, maximum possible value. If None, estimate or set a proxy.

    Returns:
    - A new vector with LDP noise applied.
    """
    noisy_vector = np.zeros_like(vector, dtype=float)

    # Estimate upper bound if not provided
    if upper_bound is None:
        upper_bound = np.max(vector)  # Replace with a domain-specific or heuristic-based value

    for i, value in enumerate(vector):
        if value > lower_bound:  # Skip values below the lower bound
            laplace_mechanism = Laplace(epsilon=epsilon, sensitivity=1)
            noisy_value = laplace_mechanism.randomise(value)
            # Clip to ensure valid range
            noisy_vector[i] = max(lower_bound, min(noisy_value, upper_bound))

    # Convert to integers
    noisy_vector = np.round(noisy_vector)  # Round to nearest integer
    noisy_vector = np.clip(noisy_vector, lower_bound, upper_bound)  # Clip to valid range
    return noisy_vector.astype(int) 


def debug_ldp_information(
    sparse_data, ldp_vector, epsilon, upper_bound, original_non_zero_indexes, ldp_non_zero_indexes
):
    """
    Print debug information to understand the impact of LDP.

    Parameters:
    - sparse_data: Original sparse vector.
    - ldp_vector: Noisy vector after applying LDP.
    - epsilon: Privacy budget.
    - upper_bound: Maximum possible value in the data.
    - original_non_zero_indexes: Non-zero indexes in the original data.
    - ldp_non_zero_indexes: Non-zero indexes in the LDP data.
    """
    # Calculate statistics
    original_sum = np.sum(sparse_data)
    ldp_sum = np.sum(ldp_vector)
    original_non_zero_count = len(original_non_zero_indexes)
    ldp_non_zero_count = len(ldp_non_zero_indexes)

    added_indexes = set(ldp_non_zero_indexes) - set(original_non_zero_indexes)
    removed_indexes = set(original_non_zero_indexes) - set(ldp_non_zero_indexes)

    # Analyze noise for a sample of non-zero entries
    sampled_indexes = original_non_zero_indexes[:5]  # Sample up to 5 non-zero indexes for analysis
    noise_analysis = [
        (idx, sparse_data[idx], ldp_vector[idx], ldp_vector[idx] - sparse_data[idx])
        for idx in sampled_indexes
    ]

    # Print debug information
    print("==== LDP Debug Information ====")
    print(f"Privacy budget (epsilon): {epsilon}")
    print(f"Upper bound: {upper_bound}")
    print(f"Vector shape: {sparse_data.shape}")
    print("\n-- Basic Statistics --")
    print(f"Original sum: {original_sum}")
    print(f"LDP sum: {ldp_sum}")
    print(f"Absolute change in sum: {ldp_sum - original_sum}")
    print(f"Relative change in sum: {(ldp_sum - original_sum) / original_sum:.2%}")
    print("\n-- Non-Zero Analysis --")
    print(f"Original non-zero count: {original_non_zero_count}")
    print(f"LDP non-zero count: {ldp_non_zero_count}")
    print(f"Added non-zero indexes: {added_indexes}")
    print(f"Removed non-zero indexes: {removed_indexes}")
    print("\n-- Noise Analysis (Sampled Non-Zero Entries) --")
    print("Index | Original Value | LDP Value | Noise")
    for idx, orig_val, ldp_val, noise in noise_analysis:
        print(f"{idx:5d} | {orig_val:14.2f} | {ldp_val:9.2f} | {noise:6.2f}")
    print("\n==== End of Debug Information ====")


def run_top5_dp(sparse_vector: Path, restricted_public_folder: Path, verbose=False):
    sparse_data = np.load(sparse_vector)
    epsilon = 0.5  # Adjust privacy budget as needed
    upper_bound = np.max(sparse_data)   # upper-bound set to the maximum number of seen episodes for a certain series

    # Apply Local Differential Privacy
    ldp_vector = apply_ldp_to_sparse_vector(sparse_data, epsilon, lower_bound=0, upper_bound=upper_bound)

    save_path = restricted_public_folder / "top5_series_dp.npy"
    np.save(save_path, ldp_vector)
    print(f">> (Top-5 Series DP | Participant) -> LDP vector saved to: {save_path}")
    
    # Non-zero index analysis
    original_non_zero_indexes = np.nonzero(sparse_data)[0]
    ldp_non_zero_indexes = np.nonzero(ldp_vector)[0]

    if verbose:
        debug_ldp_information(
            sparse_data, ldp_vector, epsilon, upper_bound, original_non_zero_indexes, ldp_non_zero_indexes
        )
    