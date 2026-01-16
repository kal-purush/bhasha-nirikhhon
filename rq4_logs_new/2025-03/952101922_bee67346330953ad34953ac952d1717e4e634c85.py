import cv2
import mediapipe as mp
import numpy as np
import os

# Load MediaPipe face mesh
mp_face_mesh = mp.solutions.face_mesh
face_mesh = mp_face_mesh.FaceMesh(static_image_mode=True, max_num_faces=1)

# Path to dataset
test_dir = "test"  # Parent directory containing emotion folders
save_dir = "selected_landmarks_test"  # Directory to save landmarks

# Create save directory if it doesn't exist
os.makedirs(save_dir, exist_ok=True)

# Define 90 landmark indices (eyes, mouth, and jawline)
selected_landmark_indices = [
    # Left Eye (Upper & Lower)
    246, 161, 160, 159, 158, 157, 173, 263, 249, 390, 373, 374, 380, 381, 382, 362,
    # Right Eye (Upper & Lower)
    466, 388, 387, 386, 385, 384, 398, 33, 7, 163, 144, 145, 153, 154, 155, 133,
    # Mouth Outer Border
    61, 146, 91, 181, 84, 17, 314, 405, 321, 375, 291, 409, 270, 269, 267, 0, 37, 39, 40, 185,
    # Mouth Inner Border
    78, 95, 88, 178, 87, 14, 317, 402, 318, 324, 308, 415, 310, 311, 312, 13, 82,
    # Full Jawline (Ear to Ear)
    234, 93, 132, 58, 172, 136, 150, 149, 176, 148, 152, 377, 400, 378, 379, 365, 397, 288, 361, 323, 454
]

# Get list of emotion categories (subfolders)
emotion_folders = [f for f in os.listdir(test_dir) if os.path.isdir(os.path.join(test_dir, f))]

# Dictionary to store landmarks for each emotion
landmarks_dict = {emotion: [] for emotion in emotion_folders}

# Iterate over each emotion folder
for emotion in emotion_folders:
    emotion_path = os.path.join(test_dir, emotion)
    print(f"Processing {emotion} images...")

    # Iterate over images in the emotion folder
    for img_name in os.listdir(emotion_path):
        img_path = os.path.join(emotion_path, img_name)

        # Load 48x48 grayscale image
        img = cv2.imread(img_path, cv2.IMREAD_GRAYSCALE)
        if img is None:
            print(f"Skipping {img_path}, failed to load.")
            continue

        # Convert to RGB
        img_rgb = cv2.cvtColor(img, cv2.COLOR_GRAY2RGB)

        # Process face
        results = face_mesh.process(img_rgb)

        # Check if face landmarks detected
        if not results.multi_face_landmarks:
            print(f"No face detected in {img_path}!")
            continue

        # Extract only the selected 90 landmarks
        for face_landmarks in results.multi_face_landmarks:
            selected_landmarks = [
                (int(face_landmarks.landmark[idx].x * img.shape[1]),
                 int(face_landmarks.landmark[idx].y * img.shape[0]))
                for idx in selected_landmark_indices
            ]
            landmarks_dict[emotion].append(selected_landmarks)  # Append to emotion category

# Convert lists to NumPy arrays and save them
for emotion, landmarks_list in landmarks_dict.items():
    if landmarks_list:  # Ensure we have data before saving
        landmarks_array = np.array(landmarks_list, dtype=np.int32)  # Convert to NumPy array
        np.save(os.path.join(save_dir, f"{emotion}_landmarks.npy"), landmarks_array)  # Save as .npy file
        print(f"Saved {emotion}_landmarks.npy with shape {landmarks_array.shape}")

print("Landmark extraction completed successfully!")