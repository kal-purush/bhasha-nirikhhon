import streamlit as st
import cv2
import face_recognition as frg
from ultralytics import YOLO
import numpy as np
import time
import os
import base64
import requests
from utils import recognize, check_and_alert

# ---------- Configuration ----------
PI_CAM_URL = "http://172.20.10.10:5000/video_feed"
YOLO_MODEL_PATH = r"D:\anomaly\runs\detect\train\weights\best.pt"
SAMPLE_IMAGE_DIR = "sample_images"

# ---------- Page and Background ----------
st.set_page_config(page_title="Intruder Detection", layout="wide")
def set_background(image_path):
    with open(image_path, "rb") as img_file:
        encoded = base64.b64encode(img_file.read()).decode()
    st.markdown(f"""
        <style>
        .stApp {{
            background-image: url("data:image/jpg;base64,{encoded}");
            background-size: cover;
            background-position: center center;
            background-repeat: no-repeat;
        }}
        </style>
    """, unsafe_allow_html=True)
set_background(r"D:\Black-Eyes-Intruders-detection-system-main\Blackeyes\Background\main.jpg")

# ---------- Load YOLO ----------
yolo_model = YOLO(YOLO_MODEL_PATH)
yolo_model.model.names = {0: 'smoke', 1: 'fire', 2: 'knife'}

# ---------- Camera Functions ----------
def get_pi_cam_frame():
    try:
        response = requests.get(PI_CAM_URL, stream=True, timeout=2)
        bytes_data = b""
        for chunk in response.iter_content(chunk_size=1024):
            bytes_data += chunk
            a = bytes_data.find(b'\xff\xd8')
            b = bytes_data.find(b'\xff\xd9')
            if a != -1 and b != -1:
                jpg = bytes_data[a:b+2]
                bytes_data = bytes_data[b+2:]
                frame = cv2.imdecode(np.frombuffer(jpg, dtype=np.uint8), 1)
                return frame
    except:
        return None

def get_webcam_frame(cap):
    ret, frame = cap.read()
    return frame if ret else None

# ---------- State Initialization ----------
if "camera_source" not in st.session_state:
    st.session_state.camera_source = "Webcam"
if "face_cam" not in st.session_state:
    st.session_state.face_cam = False
if "anomaly_cam" not in st.session_state:
    st.session_state.anomaly_cam = False
if "last_alert_time" not in st.session_state:
    st.session_state.last_alert_time = 0  # For alert cooldown

# ---------- UI ----------
st.title("🛡️ Intruder Detection System")
mode = st.sidebar.radio("Select Mode", ["Tracking", "Anomaly Detection"])

def get_frame(source, cap):
    return get_webcam_frame(cap) if source == "Webcam" else get_pi_cam_frame()

def toggle_camera():
    st.session_state.camera_source = "Pi" if st.session_state.camera_source == "Webcam" else "Webcam"

# ---------- Tracking Mode ----------
if mode == "Tracking":
    st.subheader("🔍 Face Recognition")
    input_type = st.sidebar.selectbox("Input Type", ["Live Camera", "Upload Image"])
    TOLERANCE = st.sidebar.slider("Face Match Tolerance", 0.1, 1.0, 0.5, 0.05)
    FRAME_WINDOW = st.empty()

    if input_type == "Upload Image":
        file = st.file_uploader("Upload image", type=["jpg", "jpeg", "png"])
        if file:
            img = frg.load_image_file(file)
            out_img, name, id = recognize(img, TOLERANCE)
            st.image(out_img, channels="RGB")
            st.write(f"👤 Name: `{name}` | 🆔 ID: `{id}`")

            if name == 'Unknown':
                if time.time() - st.session_state.last_alert_time > 60:
                    alert_sent = check_and_alert(name)
                    if alert_sent:
                        st.success("✅ Alert sent!")
                        st.session_state.last_alert_time = time.time()
                    elif alert_sent is False:
                        st.error("❌ Failed to send alert.")
                else:
                    st.info("ℹ️ Alert recently sent. Please wait...")

    else:
        cap = None
        col1, col2, col3 = st.columns(3)
        if col1.button("▶️ Start"):
            st.session_state.face_cam = True
            if st.session_state.camera_source == "Webcam":
                cap = cv2.VideoCapture(0)
        if col2.button("⏹️ Stop"):
            st.session_state.face_cam = False
            if cap: cap.release()
        if col3.button("🔄 Toggle Camera"):
            toggle_camera()
            if st.session_state.face_cam and st.session_state.camera_source == "Webcam":
                cap = cv2.VideoCapture(0)
            elif cap:
                cap.release()

        while st.session_state.face_cam:
            frame = get_frame(st.session_state.camera_source, cap)
            if frame is None:
                st.warning(f"No frame from {st.session_state.camera_source}.")
                break

            rgb = cv2.cvtColor(frame, cv2.COLOR_BGR2RGB)
            out_img, name, id = recognize(rgb, TOLERANCE)

            print(f"[INFO] Detected: {name}, ID: {id}")  # Debug log

            if name == 'Unknown':
                if time.time() - st.session_state.last_alert_time > 60:
                    alert_sent = check_and_alert(name)
                    if alert_sent:
                        st.success("✅ Alert sent!")
                        st.session_state.last_alert_time = time.time()
                    elif alert_sent is False:
                        st.error("❌ Failed to send alert.")
                else:
                    st.info("ℹ️ Alert recently sent. Please wait...")

            FRAME_WINDOW.image(out_img, channels="RGB")

        if cap:
            cap.release()
        cv2.destroyAllWindows()

# ---------- Anomaly Detection ----------
elif mode == "Anomaly Detection":
    st.subheader("🔥 Anomaly Detection (YOLO)")
    input_mode = st.radio("Choose Input:", ["Live Camera", "Sample Image"])
    stframe = st.empty()

    if input_mode == "Sample Image":
        choice = st.selectbox("Choose image", ["fire", "smoke", "knife"])
        img_path = os.path.join(SAMPLE_IMAGE_DIR, f"{choice}.jpg")
        if os.path.exists(img_path):
            frame = cv2.imread(img_path)
            results = yolo_model(frame)
            for result in results:
                for box in result.boxes:
                    x1, y1, x2, y2 = map(int, box.xyxy[0])
                    conf = box.conf[0].item()
                    cls = int(box.cls[0].item())
                    label = f"{yolo_model.model.names[cls]}: {conf:.2f}"
                    cv2.rectangle(frame, (x1, y1), (x2, y2), (0,255,0), 2)
                    cv2.putText(frame, label, (x1, y1-10), cv2.FONT_HERSHEY_SIMPLEX, 0.5, (0,255,0), 2)
            st.image(cv2.cvtColor(frame, cv2.COLOR_BGR2RGB), channels="RGB")
    else:
        cap = None
        col1, col2, col3 = st.columns(3)
        if col1.button("▶️ Start Detection"):
            st.session_state.anomaly_cam = True
            if st.session_state.camera_source == "Webcam":
                cap = cv2.VideoCapture(0)
        if col2.button("⏹️ Stop Detection"):
            st.session_state.anomaly_cam = False
            if cap: cap.release()
        if col3.button("🔄 Toggle Camera"):
            toggle_camera()
            if st.session_state.anomaly_cam and st.session_state.camera_source == "Webcam":
                cap = cv2.VideoCapture(0)
            elif cap:
                cap.release()

        while st.session_state.anomaly_cam:
            frame = get_frame(st.session_state.camera_source, cap)
            if frame is None:
                st.warning(f"No frame from {st.session_state.camera_source}.")
                break

            results = yolo_model(frame)
            for result in results:
                for box in result.boxes:
                    x1, y1, x2, y2 = map(int, box.xyxy[0])
                    conf = box.conf[0].item()
                    cls = int(box.cls[0].item())
                    label = f"{yolo_model.model.names[cls]}: {conf:.2f}"
                    cv2.rectangle(frame, (x1, y1), (x2, y2), (0,255,0), 2)
                    cv2.putText(frame, label, (x1, y1-10), cv2.FONT_HERSHEY_SIMPLEX, 0.5, (0,255,0), 2)

            stframe.image(cv2.cvtColor(frame, cv2.COLOR_BGR2RGB), channels="RGB")

        if cap:
            cap.release()
        cv2.destroyAllWindows()