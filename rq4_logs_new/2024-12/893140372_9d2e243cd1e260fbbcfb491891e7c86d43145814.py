import numpy as np

def initialize_item_factors(tv_vocab: dict, imdb_ratings: dict, latent_dim: int = 10, random_seed: int = 42) -> np.ndarray:
    """
    Initialize item factors using TV vocabulary and IMDB ratings.
    """
    np.random.seed(random_seed)
    num_items = max(tv_vocab.values()) + 1
    default_rating = np.mean(list(imdb_ratings.values()))
    V = np.zeros((num_items, latent_dim))
    not_found = 0

    for title, idx in tv_vocab.items():
        rating = get_rating_with_fallback(title, imdb_ratings, default_rating)
        V[idx] = generate_item_vector(rating, latent_dim)
        if rating == default_rating:
            not_found += 1

    V = normalize_vectors(V)
    print(f"Initialized item factors for {num_items} items. {not_found} items not found in IMDB data.")
    return V


def get_rating_with_fallback(normalized_title: str, imdb_ratings: dict, default_rating: float) -> float:
    """
    Retrieve a rating or use the default.
    """
    return imdb_ratings.get(normalized_title, default_rating)


def generate_item_vector(rating: float, latent_dim: int) -> np.ndarray:
    """
    Generate an item vector with noise, ensuring values stay within a defined range.
    """
    base_vector = np.full(latent_dim, rating)
    noise = np.random.normal(scale=0.2 * rating, size=latent_dim)
    vector = base_vector + noise

    # Clamp values to stay within reasonable bounds
    min_val = rating - 0.2 * rating
    max_val = rating + 0.2 * rating
    return np.clip(vector, min_val, max_val)


def normalize_vectors(V: np.ndarray) -> np.ndarray:
    """
    Normalize rows of a matrix to unit length.
    """
    norms = np.linalg.norm(V, axis=1, keepdims=True)
    return np.divide(V, norms, where=(norms != 0))