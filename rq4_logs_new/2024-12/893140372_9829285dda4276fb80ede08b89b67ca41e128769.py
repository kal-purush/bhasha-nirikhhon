import numpy as np
import matplotlib.pyplot as plt

def calculate_optimal_threshold(delta_V, method="median", percentile=90):
    """
    Calculate the optimal threshold for clipping deltas.

    Parameters:
        delta_V (dict): Dictionary of deltas (e.g., gradients or parameter updates).
        method (str): Method for calculating the threshold. Options: "mean", "median", "percentile".
        percentile (int): Percentile to use if `method="percentile"`. Defaults to 90.

    Returns:
        float: Optimal threshold based on the chosen method.
    """
    norms = [np.linalg.norm(delta) for delta in delta_V.values()]

    if method == "mean":
        return np.mean(norms)
    elif method == "median":
        return np.median(norms)
    else:
        raise ValueError(f"Invalid method '{method}'. Choose from 'mean', 'median', or 'percentile'.")

def clip_deltas(delta_V, clipping_threshold=None, method="median"):
    """
    Clip deltas based on a specified or calculated threshold.

    Parameters:
        delta_V (dict): Dictionary of deltas to clip.
        clipping_threshold (float, optional): Clipping threshold. If None, it will be computed.
        method (str): Method for calculating the threshold. Options: "mean", "median".

    Returns:
        dict: Clipped deltas.
        float: Clipping threshold used.
    """
    # Calculate optimal clipping threshold if not provided
    if clipping_threshold is None:
        clipping_threshold = calculate_optimal_threshold(delta_V, method=method)

    # Apply clipping
    for item_id, delta in delta_V.items():
        norm = np.linalg.norm(delta)
        if norm > clipping_threshold:
            delta_V[item_id] = (delta / norm) * clipping_threshold

    return delta_V, clipping_threshold

def apply_differential_privacy(delta_V, epsilon, sensitivity):
    """
    Apply differential privacy to the deltas by adding noise.

    Parameters:
        delta_V (dict): Dictionary of deltas.
        epsilon (float): Privacy budget.
        sensitivity (float): Sensitivity of the deltas (L2 norm after clipping).

    Returns:
        dict: Differentially private deltas.
    """
    # Calculate noise scale
    noise_scale = sensitivity / epsilon

    # Add noise
    for item_id, delta in delta_V.items():
        norm = np.linalg.norm(delta)
        if norm > 0:  # Avoid division by zero
            delta /= norm
            noise = np.random.normal(scale=noise_scale, size=delta.shape)
            delta += noise
            delta *= norm
            delta_V[item_id] = delta

    return delta_V

def plot_delta_distributions(delta_norms_before, delta_norms_after, clipping_threshold=0.8):
    """
    Plot the distribution of delta norms before and after differential privacy.
    """
    delta_norms_after_clipped = np.clip(delta_norms_after, 0, clipping_threshold)
    plt.figure(figsize=(12, 6))
    plt.hist(delta_norms_before, bins=50, alpha=0.7, label="Before DP Noise", color="blue", density=True)
    plt.hist(delta_norms_after_clipped, bins=50, alpha=0.7, label="After DP Noise", color="red", density=True)
    plt.title("Distribution of Delta Norms Before and After Differential Privacy Noise", fontsize=14)
    plt.xlabel("Delta Norms", fontsize=12)
    plt.ylabel("Density", fontsize=12)
    plt.legend(fontsize=12)
    plt.grid(alpha=0.3)
    plt.show()