import os
import subprocess

# Define the paths
droid_script_path = "/home/jovyan/work/dca/droid/droid.sh"
dataset_to_analyze = "chapelle-saint-loup-12-139-abcde-c-dessins"
folder_to_analyze = f"/home/jovyan/work/dca/data/{dataset_to_analyze}"
output_folder = f"/home/jovyan/work/dca/data/{dataset_to_analyze}_results"
output_csv_path = f"{output_folder}/analysis_result.csv"
new_dataset_name = f"{dataset_to_analyze}_results"

try:
    # Create the output folder if it doesn't exist
    os.makedirs(output_folder, exist_ok=True)

    # Run the droid script with the specified folder and output CSV file
    subprocess.run([droid_script_path, "-R", folder_to_analyze, "-o", output_csv_path], check=True)

    # Create the new dataset if it doesn't exist
    subprocess.run(["renku", "dataset", "create", new_dataset_name], check=True)

    # Add the output CSV file to the new dataset and overwrite if it exists
    subprocess.run(["renku", "dataset", "add", "--overwrite", "--copy", new_dataset_name, output_csv_path], check=True)

    # Commit the changes
    subprocess.run(["git", "add", "."], check=True)
    subprocess.run(["git", "commit", "-m", f"Added {output_csv_path} to {new_dataset_name} dataset"], check=True)

    # Pull the latest changes from the remote repository
    subprocess.run(["git", "pull", "--rebase"], check=True)

    # Push the changes to the remote repository
    subprocess.run(["git", "push"], check=True)

    print(f"Analysis complete. The result is saved in {output_csv_path} and added to the dataset {new_dataset_name}. Changes have been committed and pushed.")

except subprocess.CalledProcessError as e:
    print(f"An error occurred: {e}")