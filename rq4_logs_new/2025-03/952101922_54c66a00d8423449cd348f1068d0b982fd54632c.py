import cv2
import mediapipe as mp
import numpy as np
import os

# Load MediaPipe face mesh
mp_face_mesh = mp.solutions.face_mesh
face_mesh = mp_face_mesh.FaceMesh(static_image_mode=True, max_num_faces=1)

# Path to dataset
train_dir = "test"  # Parent directory containing emotion folders

# Get list of emotion categories (subfolders)
emotion_folders = [f for f in os.listdir(train_dir) if os.path.isdir(os.path.join(train_dir, f))]

# Dictionary to store landmarks for each emotion
landmarks_dict = {emotion: [] for emotion in emotion_folders}

# Iterate over each emotion folder
for emotion in emotion_folders:
    emotion_path = os.path.join(train_dir, emotion)
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

        # Store landmark points
        for face_landmarks in results.multi_face_landmarks:
            landmarks = [
                (int(landmark.x * img.shape[1]), int(landmark.y * img.shape[0]))
                for landmark in face_landmarks.landmark
            ]
            landmarks_dict[emotion].append(landmarks)  # Append to emotion category

# Convert lists to NumPy arrays and save them
for emotion, landmarks_list in landmarks_dict.items():
    landmarks_array = np.array(landmarks_list, dtype=np.int32)  # Convert to NumPy array
    np.save(f"data\\landmarks_test\\{emotion}_landmarks.npy", landmarks_array)  # Save as .npy file
    print(f"Saved {emotion}_landmarks.npy with shape {landmarks_array.shape}")

print("Landmark extraction completed successfully!")