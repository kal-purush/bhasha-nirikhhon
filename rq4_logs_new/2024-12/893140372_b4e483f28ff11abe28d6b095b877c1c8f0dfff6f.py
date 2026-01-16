import numpy as np
from datetime import datetime
from collections import Counter

## ==================================================================================================
## Data Processing (1) - Reduction
## ==================================================================================================

def extract_titles(history: np.ndarray) -> np.ndarray:
    """
    Extract and reduce titles from the viewing history.
    """
    return np.array([title.split(":")[0] if ":" in title else title for title in history[:, 0]])

def convert_dates_to_weeks(history: np.ndarray) -> np.ndarray:
    """
    Convert viewing dates to ISO week numbers.
    """
    return np.array([
        datetime.strptime(date, "%d/%m/%Y").isocalendar()[1]
        for date in history[:, 1]
    ])

def orchestrate_reduction(history: np.ndarray) -> np.ndarray:
    """
    Orchestrates the reduction process for Netflix viewing history.
    """
    titles = extract_titles(history)
    weeks = convert_dates_to_weeks(history)
    return np.column_stack((titles, weeks))

## ==================================================================================================
## Data Processing (2) - Data Enrichment (shows)
## ==================================================================================================

def join_viewing_history_with_netflix(reduced_history, netflix_show_data):
    """
    Join the reduced viewing history with Netflix show data based on titles.

    Args:
        reduced_history (np.ndarray): Viewing history data.
        netflix_show_data (np.ndarray): Netflix titles data.

    Returns:
        list: Joined data as a list of dictionaries with keys from both datasets.
    """
    # Reduce the viewing history to relevant titles
    my_titles = set([str(title) for title in reduced_history[:, 0]])

    # Get Netflix show titles
    netflix_titles_dict = {
        str(row[2]): row for row in netflix_show_data
    }  # Use title as key for efficient lookups

    # Perform an inner join
    joined_rows = []

    for row in reduced_history:
        title = str(row[0])
        if title in netflix_titles_dict:
            netflix_row = netflix_titles_dict[title]
            # Combine viewing history row and Netflix data row
            joined_rows.append(np.concatenate((row, netflix_row)))

    joined_data = np.array(joined_rows)
    my_found_titles = set([str(title) for title in joined_data[:, 0]])

    print(f"Found Titles: {len(my_found_titles)}")
    print(f"Not Found Titles: {len(my_titles) - len(my_found_titles)}")

    return joined_data

## ==================================================================================================
## Data Processing (3) - Viewing History Aggregation and Storage
## ==================================================================================================

def aggregate_title_week_counts(reduced_data: np.ndarray) -> np.ndarray:
    """
    Aggregate the reduced viewing history by counting occurrences for each title and week.

    Args:
        reduced_data (np.ndarray): A 2D array with titles and weeks.

    Returns:
        np.ndarray: A 2D array with aggregated counts for each title and week combination.
    """
    counts = Counter(map(tuple, reduced_data))
    aggregated_data = np.array([[title, week, str(count)] for (title, week), count in counts.items()])
    return aggregated_data

def aggregate_and_store_history(reduced_history, viewing_history, private_folder, restricted_public_folder):
    """
    Process and save the reduced, aggregated, and full viewing history.

    Args:
        reduced_history: Reduced viewing history data.
        viewing_history: Full viewing history data.
        private_folder: Path to the private folder.
        restricted_public_folder: Path to the restricted public folder.
    """
    # Aggregate the reduced information
    aggregated_history = aggregate_title_week_counts(reduced_history)

    # Define paths
    public_reduced_file = restricted_public_folder / "netflix_reduced.npy"
    public_aggregated_file = restricted_public_folder / "netflix_aggregated.npy"
    private_full_file = private_folder / "netflix_full.npy"

    # Save reduced viewing history
    np.save(str(public_reduced_file), reduced_history)

    # Save aggregated viewing history
    np.save(str(public_aggregated_file), aggregated_history)

    # Save full viewing history
    np.save(str(private_full_file), viewing_history)

    return aggregated_history