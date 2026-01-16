import pandas as pd
import os

# Define paths
v1_path = "/home/yulab/Desktop/Novice 01/Timeseries Data, Nov1, procedure2, vid1/2024-09-26_15-43-35-27794eea"
v2_path = "/home/yulab/Desktop/Novice 01/Timeseries Data, Nov1, procedure2, vid2/2024-09-26_16-34-57-38151d66"
output_path = "/home/yulab/Desktop/Novice 01/N1_P2"

# Ensure output directory exists
os.makedirs(output_path, exist_ok=True)

# Files to combine
files = ["gaze.csv", "saccades.csv", "fixations.csv"]

# Combine each file
for file in files:
    v1_df = pd.read_csv(os.path.join(v1_path, file))
    v2_df = pd.read_csv(os.path.join(v2_path, file))
    combined_df = pd.concat([v1_df, v2_df], ignore_index=True)
    combined_df.to_csv(os.path.join(output_path, file), index=False)

print("Files successfully combined and saved to:", output_path)