import json
import os
import numpy as np
from pathlib import Path

API_NAME = os.getenv("API_NAME")
AGGREGATOR_DATASITE = os.getenv("AGGREGATOR_DATASITE")

def calculate_top5(files: list[Path], destination_folder: Path, vocab: Path):
    """
    Calculates the top-5 most seen (number of episodes) series.
    """
    data = []

    for vector in files:
        if vector.is_file(): data.append(np.load(vector))

    result = np.vstack(data)
    series_totals = result.sum(axis=0)

    # Load the series mapping (index to name) from the JSON file
    try:
        with open(vocab, 'r') as f:
            series_mapping = json.load(f)

        # Reverse the mapping to go from index to name
        index_to_name = {v: k for k, v in series_mapping.items()}
    except:
        print(f"> Error: {API_NAME} | Aggregator: {AGGREGATOR_DATASITE} | Unable to open vocab -> {str(vocab)}")

    destination_folder.mkdir(parents=True, exist_ok=True)
    # Get the indices of the top-5 most-watched series
    top5_indices = np.argsort(series_totals)[-5:][::-1]
    top5_names = [index_to_name[idx] for idx in top5_indices]
    top5_values = series_totals[top5_indices]

    with open(destination_folder / "top5_series.json", 'w') as f:
        json.dump({"names": top5_names, "counts": top5_values.tolist()}, f, indent=4)

    

    

def dp_top5_series(datasites_path: Path, peers: list[str], min_participants):
    """
    Retrieves the path of all available participants with DP vectors of TV series seens episodes.
    """
    available_dp_vectors = []
    dp_file = "top5_series_dp.npy"

    for peer in peers:
        dir: Path = datasites_path / peer / "api_data" / API_NAME
        file: Path =dir / dp_file
        
        if file.exists():
            available_dp_vectors.append(file)

    if len(available_dp_vectors) < min_participants:
        print(f"{API_NAME} | Aggregator | There are no sufficient partcipants \
              (Available: {len(available_dp_vectors)}| Required: {min_participants})")
    else:
        destination_folder: Path = ( datasites_path / AGGREGATOR_DATASITE / "private" / API_NAME )
        vocab: Path = datasites_path / peer / "api_data" / API_NAME / "tv-series_vocabulary.json"
        calculate_top5(available_dp_vectors, destination_folder, vocab)