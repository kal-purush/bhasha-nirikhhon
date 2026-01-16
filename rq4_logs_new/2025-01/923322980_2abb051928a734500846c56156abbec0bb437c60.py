import os
import numpy as np
import json
from src.utils.video_processing import capture_frames_from_folder, preprocess_frames, detect_motion_in_frames

# Path to the directory containing frames
raw_data_dir = "data/raw/"
output_dir = "data/processed/"

# Initialize lists to store frames and labels
all_frames = []
all_labels = []

# Initialize metadata dictionary to store video info
metadata = {}

# Iterate over each folder in raw data (e.g., 'bear', 'jump', etc.)
for folder_name in os.listdir(raw_data_dir):
    folder_path = os.path.join(raw_data_dir, folder_name)
    
    if os.path.isdir(folder_path):  # Only process if it's a directory
        # Capture frames from the folder
        frames = capture_frames_from_folder(folder_path)
        
        # Check if frames are captured correctly
        print(f"Captured {len(frames)} frames from {folder_name}.")
        
        # Preprocess frames (if necessary)
        frames = preprocess_frames(frames)
        
        # Check if frames are preprocessed correctly
        print(f"Preprocessed frames: {len(frames)} for {folder_name}.")
        
        # Detect motion in the frames
        motion_labels = detect_motion_in_frames(frames)
        
        # Check if motion labels are generated correctly
        print(f"Motion labels generated for {folder_name}: {motion_labels[:10]}...")  # Display first 10 labels for debugging
        
        # Store the frames and corresponding labels
        all_frames.append(frames)
        all_labels.append(motion_labels)
        

# Convert lists to numpy arrays
all_frames = np.concatenate(all_frames, axis=0)  # Concatenate all frames
all_labels = np.concatenate(all_labels, axis=0)  # Concatenate all labels

# Save the frames and labels to .npy files
if not os.path.exists(output_dir):
    os.makedirs(output_dir)

# Save frames and labels as .npy files
np.save(os.path.join(output_dir, 'frames.npy'), all_frames)
np.save(os.path.join(output_dir, 'labels.npy'), all_labels)



print("Labeled dataset created successfully!")