import os
import numpy as np
import logging
from pathlib import Path

# Set up logging
logging.basicConfig(
    format="%(asctime)s - %(levelname)s - %(message)s",
    level=logging.DEBUG,
    datefmt="%Y-%m-%d %H:%M:%S",
)

def get_users_svd_deltas(
        datasites_path: Path, api_name:str, peers: list[str]
) -> tuple[list, list]:
    """
    """

    result = []
    for peer in peers:
        dir = datasites_path / peer / "api_data" / api_name

        # Iterate through all profiles. Get all folders that start with "profile_"
        flr_prefix = "profile_"
        profiles = [f for f in os.listdir(dir) if os.path.isdir(os.path.join(dir, f)) and f.startswith(flr_prefix)]

        # Sort the profiles by the number at the end of the folder name
        profiles = sorted(profiles, key=lambda x: int(x.split("_")[-1]))

        for profile in profiles:
            profile_dir = dir / profile / "svd_training"
            delta_v_path = profile_dir / "delta_V.npy"
            delta_v_success_path = profile_dir / "global_finetuning_succeed.log"
            if not delta_v_path.exists():
                logging.debug(f"Delta V not found for {profile}. Skipping...")
                continue
            
            if delta_v_success_path.exists():
                logging.debug(f"Delta V already processed for {profile}. Skipping...")
                continue
            
            logging.debug(f"Loading delta V for {profile}...")
            delta_V = np.load(delta_v_path, allow_pickle=True).item()
            result.append(delta_V)

            # Remove the delta_V.npy file and log date for update
            # os.remove(delta_v_path)
            logging.debug(f"Delta V loaded for aggregation and [optionally] removed for {profile}.")

            # Create log file in the profile directory with today's date
            with open(delta_v_success_path, "w") as f:
                f.write(f"Participant {peer} - {profile} training results aggregated in global server.")

    return result


def server_initialization(save_to: str, tv_series_path: str, imdb_ratings_path: str):
    from participant.server_utils.data_loading import load_tv_vocabulary, load_imdb_ratings
    from participant.federated_learning.svd_server_initialisation import initialize_item_factors

    def normalize_string(s):
        return s.replace('\u200b', '').lower()

    logging.info("Starting server initialization...")

    # Step 1: Load vocabulary and IMDB ratings
    logging.debug("Loading TV series vocabulary and IMDB ratings...")
    tv_vocab = load_tv_vocabulary(tv_series_path)
    imdb_ratings = load_imdb_ratings(imdb_ratings_path)

    # Step 2: Normalize IMDB ratings
    logging.debug("Normalizing IMDB ratings...")
    imdb_data = np.load(imdb_ratings_path, allow_pickle=True).item()
    imdb_ratings = {normalize_string(title): float(rating) for title, rating in imdb_data.items() if rating}

    # Step 3: Initialize item factors
    logging.debug("Initializing SVD item factors...")
    V = initialize_item_factors(tv_vocab, imdb_ratings)

    # Step 4: Save the initialized model
    os.makedirs(save_to, exist_ok=True)
    np.save(os.path.join(save_to, "global_V.npy"), V)

    logging.info("SVD Server initialization complete. Item factors saved.")

def server_aggregate(updates, save_to, weights=None, learning_rate=1.0, epsilon=1.0, clipping_threshold=0.5):
    """
    Orchestrates the server aggregation process:
    1. Loads current global item factors.
    2. Calls `aggregate_item_factors` to perform the aggregation.
    3. Saves the updated global item factors.

    Args:
        updates (list[dict]): List of delta dictionaries from participants.
        weights (list[float]): List of weights for each participant. If None, equal weights are assumed.
        learning_rate (float): Scaling factor for the aggregated deltas.
        epsilon (float): Privacy budget for differential privacy.
        clipping_threshold (float): Clipping threshold for updates.
        save_to (str): Path to save the updated global item factors.
    """
    from participant.server_utils.data_loading import load_global_item_factors
    from participant.federated_learning.svd_server_aggregation import aggregate_item_factors

    global_V_path = os.path.join(save_to, "global_V.npy")
    logging.info("Starting SVD Server aggregation...")

    # Step 1: Load current global item factors
    logging.debug("Loading current global item factors...")
    V = load_global_item_factors(global_V_path)

    # Step 2: Aggregate updates
    logging.debug("Aggregating updates...")
    V = aggregate_item_factors(
        V, updates, weights=weights, learning_rate=learning_rate, epsilon=epsilon, clipping_threshold=clipping_threshold
    )

    # Step 3: Save the updated global item factors
    os.makedirs(os.path.dirname(global_V_path), exist_ok=True)
    np.save(global_V_path, V)
    logging.info("SVD Server aggregation complete. Global item factors updated.")


def svd_engine_init_and_aggregate(
        datasites_path: Path, 
        shared_folder_path: Path,
        API_NAME: str, 
        peers: list[str],
        svd_init: bool=False):

    try:
        logging.info(f"Starting SVD Engine for Personalized Recommendations...")
        # Check if global V exists
        if svd_init or not os.path.exists(shared_folder_path / "global_V.npy"):
            logging.warning("Global item factors for SVD engine not found. Initializing server...")
            server_initialization(
                save_to=shared_folder_path,
                tv_series_path=shared_folder_path / "tv-series_vocabulary.json",
                imdb_ratings_path="data/imdb_ratings.npy"
            )

        # SVD Aggregation
        logging.info("Checking for SVD delta V updates from participants...")
        # Load delta V from participants
        delta_V_list = []
        delta_V_list = get_users_svd_deltas(datasites_path, API_NAME, peers)

        if delta_V_list:
            logging.info(f"Aggregating delta V updates for {len(delta_V_list)} profiles...")
            server_aggregate(delta_V_list, save_to=shared_folder_path, epsilon=None, clipping_threshold=None)
        else:
            logging.warning("No delta V updates found. Skipping aggregation.")
    except Exception as e:
        logging.error(f"An unexpected error occurred during SVD proces: {e}")