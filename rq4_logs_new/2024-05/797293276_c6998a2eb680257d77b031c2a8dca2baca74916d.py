import cv2
import mediapipe as mp
import numpy as np
from sklearn.metrics.pairwise import cosine_similarity


# Function to calculate cosine similarity between two pose vectors
def calculate_similarity(pose1, pose2):
    similarity = cosine_similarity([pose1.flatten()], [pose2.flatten()])
    return similarity[0][0]


# Function to extract pose landmarks from frame
def extract_pose_landmarks(frame, mp_pose):
    with mp_pose.Pose(static_image_mode=False, min_detection_confidence=0.5, min_tracking_confidence=0.5) as pose:
        frame_rgb = cv2.cvtColor(frame, cv2.COLOR_BGR2RGB)
        results = pose.process(frame_rgb)

        if results.pose_landmarks:
            landmarks = np.array([[lm.x, lm.y, lm.z] for lm in results.pose_landmarks.landmark])
            return landmarks
        else:
            return None


# Load MediaPipe Pose model
mp_pose = mp.solutions.pose

# Open the video files
video1_path = '1.mp4'
video2_path = '2.mp4'
cap1 = cv2.VideoCapture(video1_path)

cap2 = cv2.VideoCapture(video2_path)

# Initialize variables to store poses
pose1 = None
pose2 = None

# Read frames from the videos
while True:
    ret1, frame1 = cap1.read()
    ret2, frame2 = cap2.read()

    if not (ret1 and ret2):
        break

    # Extract pose landmarks from frames
    pose1 = extract_pose_landmarks(frame1, mp_pose)
    pose2 = extract_pose_landmarks(frame2, mp_pose)

    if pose1 is not None and pose2 is not None:
        # Calculate similarity between poses
        similarity = calculate_similarity(pose1, pose2)

        # Display the similarity score
        cv2.putText(frame1, f'Similarity: {similarity:.2f}', (50, 50), cv2.FONT_HERSHEY_SIMPLEX, 1, (0, 255, 0), 2)

        # Display frames
        cv2.imshow('Video 1', frame1)
        cv2.imshow('Video 2', frame2)

    if cv2.waitKey(1) & 0xFF == ord('q'):
        break

# Release video capture objects and close windows
cap1.release()
cap2.release()
cv2.destroyAllWindows()