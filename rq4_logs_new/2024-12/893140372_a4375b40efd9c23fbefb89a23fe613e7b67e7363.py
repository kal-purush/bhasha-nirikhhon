# Aggregator Script for Movie Ratings with Enhanced Visualization
# This script aggregates user updates (Non-DP and DP) and visualizes the results.

import numpy as np
import os
import matplotlib.pyplot as plt


# Function to load user updates from files
def load_user_updates(input_dir, num_users, num_movies, ignore_user_list:list=None):
    """
    Load Non-DP and DP updates for all users.

    Args:
        input_dir (str): Directory containing user updates as .npy files.
        num_users (int): Number of users to aggregate updates from.

    Returns:
        tuple: Aggregated Non-DP updates and DP updates as arrays.
    """
    if not ignore_user_list:
        ignore_user_list = []

    n_ignore = len(ignore_user_list)
    aggregated_non_dp = np.zeros(num_movies)  # Initialize Non-DP aggregate
    aggregated_dp = np.zeros(num_movies)  # Initialize DP aggregate

    for user_id in range(0, num_users):
        if user_id in ignore_user_list:
            continue

        # Load updates for the current user
        non_dp_path = os.path.join(input_dir, f"user_{user_id}_non_dp.npy")
        dp_path = os.path.join(input_dir, f"user_{user_id}_dp.npy")
        delta_non_dp = np.load(non_dp_path, allow_pickle=True).item()
        delta_dp = np.load(dp_path, allow_pickle=True).item()

        # Weight updates equally (can be customized)
        weight = 1.0 / (num_users - n_ignore)
        for movie_id in delta_non_dp:
            aggregated_non_dp[movie_id] += weight * delta_non_dp[movie_id]
            aggregated_dp[movie_id] += weight * delta_dp[movie_id]

    return aggregated_non_dp, aggregated_dp


# Function to visualize aggregated updates
def visualize_aggregates(aggregated_non_dp, aggregated_dp, num_movies):
    """
    Visualize Non-DP vs DP aggregated updates across all movies.

    Args:
        aggregated_non_dp (np.array): Aggregated Non-DP updates.
        aggregated_dp (np.array): Aggregated DP updates.
        num_movies (int): Total number of movies for the x-axis.

    Returns:
        None
    """
    x = np.arange(num_movies)  # Movie indices

    plt.figure(figsize=(16, 8))
    plt.style.use('seaborn-v0_8-darkgrid')

    # Plot Non-DP and DP aggregates
    plt.plot(x, aggregated_non_dp, color="blue", label="Non-DP Aggregate", lw=1.5)
    plt.plot(x, aggregated_dp, color="orange", label="DP Aggregate", lw=1.5)

    # Highlight every 10th movie on the x-axis
    for movie_id in range(9, num_movies, 10):
        plt.axvline(x=movie_id, color="gray", linestyle="--", alpha=0.3)  # Vertical line
        plt.text(
            movie_id, max(max(aggregated_non_dp), max(aggregated_dp)) * 0.95,
            f"{movie_id}",
            fontsize=8, color="gray", alpha=0.6, ha="center"
        )

    # Add specific annotations for biased movies (55 and 75)
    for bias_movie, color, label, position in [
        (54, "green", "Bias +", 0.75),
        (74, "red", "Bias -", 0.25),
    ]:
        plt.axvline(x=bias_movie, color=color, linestyle="--", alpha=0.8, label=f"Movie {bias_movie+1} ({label})")
        plt.text(
            bias_movie, max(aggregated_non_dp) * position,
            label, color=color, fontsize=10, ha="center", bbox=dict(boxstyle="round,pad=0.3", edgecolor=color, facecolor="white", alpha=0.8)
        )

    # Add titles, labels, and legend
    plt.title("Non-DP vs DP Aggregated Updates Across Movies", fontsize=18, weight='bold')
    plt.xlabel("Movie ID", fontsize=14)
    plt.ylabel("Update Magnitude (Relative)", fontsize=14)
    plt.legend(fontsize=12)
    plt.grid(alpha=0.3)

    # Adjust layout and show the plot
    plt.tight_layout()
    plt.savefig("demo/figures/demo_movies_aggregate.png", dpi=300)
    plt.show()

# Function to visualize aggregated updates for both ignore lists
def visualize_comparison(input_dir, num_users, num_movies):
    """
    Compare aggregated updates with different ignore lists.

    Args:
        input_dir (str): Directory containing user updates as .npy files.
        num_users (int): Total number of users.
        num_movies (int): Total number of movies.

    Returns:
        None
    """
    # Load aggregated updates for both cases
    aggregated_non_dp_all, aggregated_dp_all = load_user_updates(input_dir, num_users, num_movies, ignore_user_list=None)
    aggregated_non_dp_ignore, aggregated_dp_ignore = load_user_updates(input_dir, num_users, num_movies, ignore_user_list=[0])

    x = np.arange(num_movies)  # Movie indices

    plt.style.use('seaborn-v0_8-darkgrid')
    fig, axes = plt.subplots(2, 1, figsize=(18, 12), sharex=True)

    # Plot Non-DP aggregates
    axes[0].plot(x, aggregated_non_dp_all, color="blue", label="Non-DP (All Users)", linewidth=1.5)
    axes[0].plot(x, aggregated_non_dp_ignore, color="green", label="Non-DP (Ignore User 0)", linestyle="--", linewidth=1.5)
    axes[0].set_title("Non-DP Aggregates Comparison", fontsize=16, weight='bold')
    axes[0].set_ylabel("Update Magnitude", fontsize=14)
    axes[0].legend(fontsize=12, loc="upper left")
    axes[0].grid(alpha=0.3)

    # Plot DP aggregates
    axes[1].plot(x, aggregated_dp_all, color="orange", label="DP (All Users)", linewidth=1.5)
    axes[1].plot(x, aggregated_dp_ignore, color="red", label="DP (Ignore User 0)", linestyle="--", linewidth=1.5)
    axes[1].set_title("DP Aggregates Comparison", fontsize=16, weight='bold')
    axes[1].set_xlabel("Movie ID", fontsize=14)
    axes[1].set_ylabel("Update Magnitude", fontsize=14)
    axes[1].legend(fontsize=12, loc="upper left")
    axes[1].grid(alpha=0.3)

    # Add vertical dashed lines for every 10th movie and concise labels for biased movies
    for ax in axes:
        for movie_id in range(9, num_movies, 10):
            ax.axvline(x=movie_id, color="gray", linestyle="--", alpha=0.3)
            ax.text(movie_id, max(aggregated_non_dp_all) * 0.95, f"{movie_id}", 
                    fontsize=8, color="gray", alpha=0.6, ha="center")

    # Add a global title and adjust layout
    plt.suptitle(f"Comparison of Aggregated Updates Across Movies (Non-DP vs DP) - For first {num_users} Users", fontsize=20, weight='bold')
    plt.tight_layout(rect=[0, 0, 1, 0.95])
    plt.savefig("demo/figures/demo_movies_aggregate_comparison.png", dpi=300)
    plt.show()


def visualize_aggregates_comparison(output_dir_epsilon1, output_dir_epsilon10, num_users, num_movies):
    """
    Compare aggregated updates for Non-DP and DP (epsilon=1 vs epsilon=10),
    including updates excluding user 0.

    Args:
        output_dir_epsilon1 (str): Directory containing user updates for epsilon=1.
        output_dir_epsilon10 (str): Directory containing user updates for epsilon=10.
        num_users (int): Total number of users.
        num_movies (int): Total number of movies.

    Returns:
        None
    """
    # Load aggregated updates for epsilon=1
    aggregated_non_dp_eps1, aggregated_dp_eps1 = load_user_updates(output_dir_epsilon1, num_users, num_movies, ignore_user_list=None)
    aggregated_non_dp_eps1_exclude, aggregated_dp_eps1_exclude = load_user_updates(output_dir_epsilon1, num_users, num_movies, ignore_user_list=[0])

    # Load aggregated updates for epsilon=10
    _, aggregated_dp_eps10 = load_user_updates(output_dir_epsilon10, num_users, num_movies, ignore_user_list=None)
    _, aggregated_dp_eps10_exclude = load_user_updates(output_dir_epsilon10, num_users, num_movies, ignore_user_list=[0])

    x = np.arange(num_movies)  # Movie indices

    plt.style.use('seaborn-v0_8-darkgrid')
    fig, axes = plt.subplots(3, 1, figsize=(18, 15), sharex=True)

    # Plot Non-DP aggregates
    axes[0].plot(x, aggregated_non_dp_eps1, color="blue", label="Non-DP (All Users)", linewidth=1.5)
    axes[0].plot(x, aggregated_non_dp_eps1_exclude, color="green", linestyle="--", label="Non-DP (Excl. User 0)", linewidth=1.5)
    axes[0].set_title("Non-DP Aggregates", fontsize=16, weight='bold')
    axes[0].set_ylabel("Update Magnitude", fontsize=14)
    axes[0].legend(fontsize=12, loc="upper left")
    axes[0].grid(alpha=0.3)

    # Plot DP aggregates for epsilon=10
    axes[1].plot(x, aggregated_dp_eps10, color="purple", label="DP (Epsilon=10, All Users)", linewidth=1.5)
    axes[1].plot(x, aggregated_dp_eps10_exclude, color="brown", linestyle="--", label="DP (Epsilon=10, Excl. User 0)", linewidth=1.5)
    axes[1].set_title("DP Aggregates (Epsilon=10)", fontsize=16, weight='bold')
    axes[1].set_ylabel("Update Magnitude", fontsize=14)
    axes[1].legend(fontsize=12, loc="upper left")
    axes[1].grid(alpha=0.3)

    # Plot DP aggregates for epsilon=1
    axes[2].plot(x, aggregated_dp_eps1, color="orange", label="DP (Epsilon=1, All Users)", linewidth=1.5)
    axes[2].plot(x, aggregated_dp_eps1_exclude, color="red", linestyle="--", label="DP (Epsilon=1, Excl. User 0)", linewidth=1.5)
    axes[2].set_title("DP Aggregates (Epsilon=1)", fontsize=16, weight='bold')
    axes[2].set_xlabel("Movie ID", fontsize=14)
    axes[2].set_ylabel("Update Magnitude", fontsize=14)
    axes[2].legend(fontsize=12, loc="upper left")
    axes[2].grid(alpha=0.3)

    # Add vertical dashed lines for every 10th movie for all subplots
    for ax in axes:
        for movie_id in range(9, num_movies, 10):
            ax.axvline(x=movie_id, color="gray", linestyle="--", alpha=0.3)
            ax.text(movie_id, max(aggregated_non_dp_eps1) * 0.95, f"{movie_id}", 
                    fontsize=8, color="gray", alpha=0.6, ha="center")

    # Add a global title and adjust layout
    plt.suptitle("Comparison of Aggregated Updates Across Epsilon Values (Non-DP vs DP)", fontsize=20, weight='bold')
    plt.tight_layout(rect=[0, 0, 1, 0.95])
    plt.savefig("demo/figures/aggregates_comparison_across_epsilons_excl_user0.png", dpi=300)
    plt.show()

# Main execution
if __name__ == "__main__":
    # Parameters
    epsilon=10.0  # Privacy budget
    num_movies = 100  # Total number of movies
    num_users = 100  # Total number of users
    input_dir = f"demo/dp_ratings_results/epsilon={epsilon}"  # Directory containing user updates

    # Step 1: Aggregate updates
    print("Loading and aggregating user updates...")
    aggregated_non_dp, aggregated_dp = load_user_updates(input_dir, num_users, num_movies, ignore_user_list=None)

    # Step 2: Visualize aggregated updates
    print("Visualizing aggregated updates...")
    # visualize_aggregates(aggregated_non_dp, aggregated_dp, num_movies)

    # Compare visualizations for different ignore lists
    print("Comparing aggregated updates...")
    # visualize_comparison(input_dir, 10, num_movies)


    output_dir_epsilon1 = "demo/dp_ratings_results/epsilon=1.0"
    output_dir_epsilon10 = "demo/dp_ratings_results/epsilon=10.0"
    # Compare aggregates across epsilons
    visualize_aggregates_comparison(output_dir_epsilon1, output_dir_epsilon10, 50, num_movies)