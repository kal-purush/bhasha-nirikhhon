import copy
import numpy as np

def validate_weights(weights, num_participants):
    """
    Validate participant weights.

    Args:
        weights (list[float]): List of weights for each participant.
        num_participants (int): Total number of participants.

    Raises:
        ValueError: If the weights are invalid (e.g., mismatched length or zero total weight).
    """
    if weights is None:
        return  # Default weights will be handled elsewhere
    if len(weights) != num_participants:
        raise ValueError("The number of weights must match the number of participants.")
    if sum(weights) == 0:
        raise ValueError("Total weight cannot be zero.")

def normalize_weights(weights, num_participants):
    """
    Normalize participant weights to sum to 1.

    Args:
        weights (list[float]): List of weights for each participant.
        num_participants (int): Total number of participants.

    Returns:
        list[float]: Normalized weights.
    """
    if weights is None:
        weights = [1.0] * num_participants  # Default to equal weights

    validate_weights(weights, num_participants)

    total_weight = sum(weights)
    return [w / total_weight for w in weights]

def clip_updates(updates, clipping_threshold):
    """
    Clip updates to the specified threshold.

    Args:
        updates (list[dict]): List of delta dictionaries.
        clipping_threshold (float): Clipping threshold.

    Returns:
        list[dict]: Clipped updates.
    """
    clipped_updates = []
    for delta_V in updates:
        clipped_delta_V = {}
        for item_id, delta in delta_V.items():
            norm = np.linalg.norm(delta)
            if norm > clipping_threshold:
                delta = (delta / norm) * clipping_threshold
            clipped_delta_V[item_id] = delta
        clipped_updates.append(clipped_delta_V)
    return clipped_updates

def add_differential_privacy_noise(aggregated_delta, epsilon, clipping_threshold):
    """
    Add differential privacy noise to the aggregated deltas.

    Args:
        aggregated_delta (dict): Aggregated deltas.
        epsilon (float): Privacy budget.
        clipping_threshold (float): Sensitivity parameter.

    Returns:
        dict: Aggregated deltas with noise.
    """
    result = copy.deepcopy(aggregated_delta)
    noise_scale = clipping_threshold / epsilon
    for item_id, delta in result.items():
        noise = np.random.normal(scale=noise_scale, size=delta.shape)
        result[item_id] += noise
    return result

def calculate_aggregated_delta(V, updates, normalized_weights, learning_rate):
    """
    Calculate the aggregated deltas using weighted updates.

    Args:
        V (np.ndarray): Current global item factors.
        updates (list[dict]): List of delta dictionaries from participants.
        normalized_weights (list[float]): List of normalized weights for each participant.
        learning_rate (float): Scaling factor for the aggregated deltas.

    Returns:
        dict: Aggregated deltas for all items.
    """
    aggregated_delta = {item_id: np.zeros_like(V[item_id]) for item_id in range(len(V))}
    for i, delta_V in enumerate(updates):
        weight = normalized_weights[i] * learning_rate
        for item_id, delta in delta_V.items():
            aggregated_delta[item_id] += weight * delta
    return aggregated_delta

def aggregate_item_factors(V, updates, weights=None, learning_rate=1.0, epsilon=1.0, clipping_threshold=0.5):
    """
    Perform aggregation of participant updates with optional clipping and differential privacy.

    Args:
        V (np.ndarray): Current global item factors.
        updates (list[dict]): List of delta dictionaries from participants.
        weights (list[float]): List of weights for each participant. If None, equal weights are assumed.
        learning_rate (float): Scaling factor for the aggregated deltas.
        epsilon (float): Privacy budget for differential privacy.
        clipping_threshold (float): Clipping threshold for updates.

    Returns:
        np.ndarray: Updated global item factors.
    """
    # Step 1: Normalize weights (validates internally)
    normalized_weights = normalize_weights(weights, len(updates))

    # Step 2: Clip updates
    if clipping_threshold:
        updates = clip_updates(updates, clipping_threshold)

    # Step 3: Calculate aggregated delta
    aggregated_delta = calculate_aggregated_delta(V, updates, normalized_weights, learning_rate)

    # Step 4: Add differential privacy noise
    if epsilon and epsilon > 0:
        aggregated_delta = add_differential_privacy_noise(aggregated_delta, epsilon, clipping_threshold)

    # Step 5: Update global item factors
    result = copy.deepcopy(V)
    for item_id, delta in aggregated_delta.items():
        result[item_id] += delta

    return result