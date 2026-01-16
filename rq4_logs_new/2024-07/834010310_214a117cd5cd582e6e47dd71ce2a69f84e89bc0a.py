import cv2
import mediapipe as mp

# Initialize MediaPipe Pose
mp_pose = mp.solutions.pose
pose = mp_pose.Pose(static_image_mode=False, min_detection_confidence=0.7, min_tracking_confidence=0.7)

# Define the exercise ranges
exercises = {
    'Squats': {
        'nose': (0.4, 0.6),
        'left_shoulder': (0.2, 0.4),
        'right_shoulder': (0.6, 0.8),
        'left_hip': (0.2, 0.4),
        'right_hip': (0.6, 0.8),
        'left_knee': (0.4, 0.6),
        'right_knee': (0.4, 0.6),
        'left_ankle': (0.2, 0.4),
        'right_ankle': (0.6, 0.8)
    },
    'Pushups': {
        'nose': (0.4, 0.6),
        'left_shoulder': (0.2, 0.4),
        'right_shoulder': (0.6, 0.8),
        'left_elbow': (0.2, 0.4),
        'right_elbow': (0.6, 0.8),
        'left_wrist': (0.2, 0.4),
        'right_wrist': (0.6, 0.8)
    }
}

def check_exercise(landmarks, exercise):
    """Check if the detected landmarks are within the specified ranges for the exercise."""
    for landmark, range in exercise.items():
        if landmark not in landmarks:
            print(f"Warning: {landmark} not found in landmarks. Skipping.")
            continue  # Skip this landmark if not found
        x = landmarks[landmark].x
        y = landmarks[landmark].y
        if x < range[0] or x > range[1] or y < range[0] or y > range[1]:
            return False
    return True

def draw_pose(frame, landmarks, exercise):
    """Draw the detected landmarks and posture status on the frame."""
    for landmark, _ in exercise.items():
        if landmark in landmarks:
            x = int(landmarks[landmark].x * frame.shape[1])
            y = int(landmarks[landmark].y * frame.shape[0])
            cv2.circle(frame, (x, y), 5, (0, 255, 0), -1)
    cv2.putText(frame, "Correct posture", (10, 30), cv2.FONT_HERSHEY_SIMPLEX, 1, (0, 255, 0), 2)

# Start video capture
cap = cv2.VideoCapture(0)
fourcc = cv2.VideoWriter_fourcc(*'XVID')
out = cv2.VideoWriter('output.avi', fourcc, 20.0, (640, 480))

while cap.isOpened():
    ret, frame = cap.read()
    if ret:
        out.write(frame)  # Write the frame to the video file
        frame_rgb = cv2.cvtColor(frame, cv2.COLOR_BGR2RGB)
        results = pose.process(frame_rgb)
        frame_bgr = cv2.cvtColor(frame_rgb, cv2.COLOR_RGB2BGR)

        if results.pose_landmarks:
            landmarks = {}
            for i, landmark in enumerate(results.pose_landmarks.landmark):
                landmarks[f'landmark_{i}'] = landmark
            
            print("Detected landmarks:", landmarks.keys())  # Debugging information

            # Check posture for Squats or Pushups
            if check_exercise(landmarks, exercises['Squats']):
                draw_pose(frame_bgr, landmarks, exercises['Squats'])
            elif check_exercise(landmarks, exercises['Pushups']):
                draw_pose(frame_bgr, landmarks, exercises['Pushups'])
            else:
                cv2.putText(frame_bgr, "Incorrect posture", (10, 30), cv2.FONT_HERSHEY_SIMPLEX, 1, (0, 0, 255), 2)

        cv2.imshow('frame', frame_bgr)

        if cv2.waitKey(1) & 0xFF == ord('q'):
            break
    else:
        break

cap.release()
out.release()
cv2.destroyAllWindows()