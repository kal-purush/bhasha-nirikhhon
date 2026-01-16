import os
import pandas as pd
from configparser import ConfigParser

def merge(source_dir: str, destination_dir: str, output_filename: str):
    """
    Merge all static metrics CSV files into a single file and add a `Version` column at the first position.

    The CSV files are located in the `source_dir`. The `Version` column is extracted from the file names.

    The final merged file is saved in the `destination_dir` with the specified output filename.

    Parameters:
        source_dir (str): Path to the directory containing the static metrics CSV files.
        destination_dir (str): Path to the directory where the merged file will be saved.
        output_filename (str): Name of the output file (e.g., "merged_static_metrics.csv").
    """
    # Load the configuration file
    config = ConfigParser()
    config.read("config.ini")

    if config['OUTPUT'].get('SkipMerge', 'No').lower() == 'yes':
        print("Merging has already been done. Skipping...")
        return

    # Retrieve the CSV separator from the configuration
    csv_separator = config["GENERAL"]["CSVSeparatorMetrics"]

    # Check if the source directory exists
    if not os.path.exists(source_dir):
        raise FileNotFoundError(f"The directory {source_dir} does not exist.")

    # List all CSV files in the directory
    csv_files = [f for f in os.listdir(source_dir) if f.endswith(".csv")]

    if not csv_files:
        raise ValueError(f"No CSV files found in the directory {source_dir}.")

    merged_data = []  # List to store DataFrames

    for csv_file in csv_files:
        # Extract the version from the file name (e.g., `2.1.0` from `2.1.0_labeled_static_metrics.csv`)
        version = os.path.splitext(csv_file)[0].split("_")[0]

        # Load the CSV file
        file_path = os.path.join(source_dir, csv_file)
        df = pd.read_csv(file_path, sep=csv_separator)

        # Add the `Version` column
        df.insert(0, "Version", version)  # Insert at the first position

        # Append the DataFrame to the list
        merged_data.append(df)

    # Concatenate all DataFrames
    merged_df = pd.concat(merged_data, ignore_index=True)

    # Sort the merged DataFrame by the `Name` column
    if "Name" in merged_df.columns and "Version" in merged_df.columns:
        merged_df = merged_df.sort_values(by=["Name", "Version"])

    # Ensure the destination directory exists
    os.makedirs(destination_dir, exist_ok=True)

    # Define the output path for the merged file
    output_file = os.path.join(destination_dir, output_filename)

    # Save the merged DataFrame to a CSV file
    merged_df.to_csv(output_file, index=False, sep=csv_separator)

    print(f"Merged file saved at: {output_file}")