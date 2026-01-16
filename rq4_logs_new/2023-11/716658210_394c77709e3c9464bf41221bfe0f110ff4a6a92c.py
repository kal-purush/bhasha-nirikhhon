import cv2
import mediapipe as mp
import numpy as np
import math
import csv
import matplotlib.pyplot as plt
# 미디어파이프 초기화
mp_drawing = mp.solutions.drawing_utils
mp_pose = mp.solutions.pose

# 미디어파이프에 정의된 관절 이름 및 관계 사용
BODY_PARTS = {
    "Nose": 0, "LeftEye": 1, "RightEye": 2, "LeftEar": 3, "RightEar": 4,
    "LeftShoulder": 5, "RightShoulder": 6, "LeftElbow": 7, "RightElbow": 8,
    "LeftWrist": 9, "RightWrist": 10, "LeftHip": 11, "RightHip": 12,
    "LeftKnee": 13, "RightKnee": 14, "LeftAnkle": 15, "RightAnkle": 16
}

POSE_CONNECTIONS = mp_pose.POSE_CONNECTIONS

# BODY_PARTS 딕셔너리 업데이트
for connection in POSE_CONNECTIONS:
    part1, part2 = connection
    if part1 not in BODY_PARTS:
        BODY_PARTS[part1] = len(BODY_PARTS)
    if part2 not in BODY_PARTS:
        BODY_PARTS[part2] = len(BODY_PARTS)

def calculate_joint_angles(landmarks):
    angles = {}

    for pair in POSE_CONNECTIONS:
        part1, part2 = pair
        id1, id2 = BODY_PARTS[part1], BODY_PARTS[part2]
        
        if id1 in landmarks and id2 in landmarks:
            x1, y1 = landmarks[id1]
            x2, y2 = landmarks[id2]
            angle_rad = math.atan2(y2 - y1, x2 - x1)
            angle_deg = math.degrees(angle_rad)
            angles[(part1, part2)] = angle_deg
    return angles

def read_joint_data(csv_filename):
    joint_data = {}

    with open(csv_filename, mode='r') as csv_file:
        csv_reader = csv.reader(csv_file)
        next(csv_reader)
        for row in csv_reader:
            frame = int(row[0])
            joint_index = int(row[1])
            x = float(row[2])
            y = float(row[3])

            if frame not in joint_data:
                joint_data[frame] = {}
            joint_data[frame][joint_index] = (x, y)

    return joint_data



def calculate_cosine_similarity(v1, v2):
    if len(v1) == 0 or len(v2) == 0:
        return 0

    max_len = max(len(v1), len(v2))
    v1 = np.pad(v1, (0, max_len - len(v1)))
    v2 = np.pad(v2, (0, max_len - len(v2)))

    dot_product = np.dot(v1, v2)
    norm_v1 = np.linalg.norm(v1)
    norm_v2 = np.linalg.norm(v2)

    if norm_v1 == 0 or norm_v2 == 0:
        return 0

    similarity = dot_product / (norm_v1 * norm_v2)
    return similarity

def save_and_print_top_frames(csv_file1, csv_file2, n=5):
    # 두 CSV 파일에서 포즈 데이터 읽어오기
    pose_data1 = read_joint_data(csv_file1)
    pose_data2 = read_joint_data(csv_file2)
    # 모든 프레임 간의 유사도 계산
    frame_similarities = []

    for frame1, landmarks1 in pose_data1.items():
        for frame2, landmarks2 in pose_data2.items():
            if not landmarks1 or not landmarks2:
                continue

            # 관절 각도 계산
            angles1 = calculate_joint_angles(landmarks1)

            angles2 = calculate_joint_angles(landmarks2)

            # 관절 각도를 벡터로 변환
            angle_vector1 = np.array(list(angles1.values()))

            angle_vector2 = np.array(list(angles2.values()))

            # 코사인 유사도 계산
            similarity = calculate_cosine_similarity(angle_vector1, angle_vector2)

            frame_similarities.append((frame1, frame2, similarity))

    # 유사도에 따라 프레임을 정렬
    frame_similarities.sort(key=lambda x: x[2], reverse=True)

    # 상위 N개 프레임 저장 및 출력
    output_path = "././data/image/"  # 이미지를 저장할 경로 지정

    for rank, (frame1, frame2, similarity) in enumerate(frame_similarities, 1):
        if similarity < 0.990:
            break  # 유사도가 99.7% 미만인 프레임이 나오면 종료

        cap1.set(cv2.CAP_PROP_POS_FRAMES, frame1)
        cap2.set(cv2.CAP_PROP_POS_FRAMES, frame2)

        ret1, frame1_img = cap1.read()
        ret2, frame2_img = cap2.read()

        if ret1 and ret2:
            cv2.imwrite(f"{output_path}sim{rank}_1.png", frame1_img)
            cv2.imwrite(f"{output_path}sim{rank}_2.png", frame2_img)
            print(f"Frames {frame1} from CSV1 and {frame2} from CSV2 (Similarity: {similarity}) saved as {output_path}sim{rank}_1.png and {output_path}sim{rank}_2.png.")
            abs(frame2-frame1)
            
        else:
            print(f"Frames {frame1} from CSV1 and {frame2} from CSV2 (Similarity: {similarity}) could not be read.")

# 두 CSV 파일 경로와 비디오 파일 경로

csv_file1 = "././.csv"
csv_file2 = "./..csv"
cap1 = cv2.VideoCapture("././.avi")
cap2 = cv2.VideoCapture("././.avi")

# 상위 5개 프레임 저장 및 출력
save_and_print_top_frames(csv_file1, csv_file2, n=100)