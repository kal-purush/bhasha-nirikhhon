import streamlit as st
import cv2
import numpy as np
import uuid
import logging
from ultralytics import YOLO
from ultralytics.nn.tasks import DetectionModel
from pathlib import Path
from PIL import Image
import pandas as pd
import torch
import os
import matplotlib.pyplot as plt
import io
from base64 import b64encode

# Setup logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

# Custom CSS for vibrant colors and improved visibility
st.markdown("""
<style>
    @import url('https://fonts.googleapis.com/css2?family=Inter:wght@300;400;500;600;700&display=swap');

    * {
        box-sizing: border-box;
        font-family: 'Inter', sans-serif;
        margin: 0;
        padding: 0;
    }

    .stApp {
        background: linear-gradient(135deg, #fefce8 0%, #e0f2fe 100%);
        width: 100vw;
        max-width: none;
        min-height: 100vh;
        padding: 3rem;
        margin: 0 auto;
        overflow-x: hidden;
    }

    h1 {
        font-family: 'Inter', sans-serif;
        font-weight: 700;
        font-size: 3.5rem;
        color: #1f2937;
        text-align: center;
        margin-bottom: 2rem;
        letter-spacing: -0.5px;
        text-shadow: 1px 1px 4px rgba(0, 0, 0, 0.05);
    }

    h2 {
        font-family: 'Inter', sans-serif;
        font-weight: 600;
        font-size: 2rem;
        color: #1f2937;
        margin-bottom: 1.5rem;
    }

    h3 {
        font-family: 'Inter', sans-serif;
        font-weight: 500;
        font-size: 1.5rem;
        color: #374151;
        margin-bottom: 1rem;
    }

    p, .stMarkdown, .stText {
        font-family: 'Inter', sans-serif;
        font-weight: 400;
        font-size: 1rem;
        color: #4b5563;
        line-height: 1.6;
        margin-bottom: 1rem;
    }

    .stSelectbox label, .stSlider label, .stNumberInput label {
        font-family: 'Inter', sans-serif;
        font-weight: 500;
        font-size: 1rem;
        color: #1f2937;
        margin-bottom: 0.5rem;
    }

    .stButton>button {
        background: linear-gradient(to right, #0d9488, #14b8a6);
        color: #ffffff;
        border: none;
        padding: 12px 24px;
        border-radius: 6px;
        font-family: 'Inter', sans-serif;
        font-weight: 600;
        font-size: 1rem;
        transition: all 0.3s ease;
        width: 100%;
        box-shadow: 0 2px 4px rgba(0, 0, 0, 0.1);
    }

    .stButton>button:hover {
        background: linear-gradient(to right, #0f766e, #0d9488);
        color: #ffffff;
        transform: translateY(-2px);
        box-shadow: 0 4px 8px rgba(0, 0, 0, 0.15);
    }

    .result-container {
        background: #ffffff;
        border-radius: 8px;
        padding: 2rem;
        box-shadow: 0 2px 10px rgba(0, 0, 0, 0.05);
        margin: 2rem 0;
        border: 1px solid #e5e7eb;
    }

    .camera-section {
        background: #ffffff;
        border-radius: 8px;
        padding: 2rem;
        box-shadow: 0 2px 10px rgba(0, 0, 0, 0.05);
        margin: 2rem 0;
        border: 1px solid #e5e7eb;
    }

    .stTabs [role="tablist"] {
        background: #f1f5f9;
        border-radius: 8px;
        padding: 0.5rem;
        display: flex;
        gap: 1rem;
        margin-bottom: 2rem;
        justify-content: center;
    }

    .stTabs [role="tab"] {
        font-family: 'Inter', sans-serif;
        font-weight: 600;
        font-size: 1rem;
        color: #4b5563;
        padding: 0.75rem 1.5rem;
        border-radius: 6px;
        transition: all 0.3s ease;
    }

    .stTabs [role="tab"][aria-selected="true"] {
        background: #0d9488;
        color: #ffffff;
        box-shadow: 0 2px 4px rgba(0, 0, 0, 0.1);
    }

    .stTabs [role="tab"]:hover {
        background: #d1d5db;
        color: #0d9488;
    }

    .stDataFrame {
        border-radius: 8px;
        overflow: hidden;
        box-shadow: 0 2px 8px rgba(0, 0, 0, 0.05);
        margin: 1.5rem 0;
    }

    .stDataFrame table {
        width: 100%;
        border-collapse: collapse;
        background: #ffffff;
    }

    .stDataFrame th {
        background: linear-gradient(to right, #0d9488, #14b8a6);
        color: #ffffff;
        padding: 12px;
        font-weight: 600;
        text-align: left;
    }

    .stDataFrame td {
        padding: 12px;
        border-bottom: 1px solid #e5e7eb;
        color: #1f2937;
    }

    .stExpander {
        background: #f9fafb;
        border-radius: 8px;
        border: 1px solid #e5e7eb;
        padding: 1.5rem;
        margin: 1.5rem 0;
    }

    .stFileUploader label {
        font-family: 'Inter', sans-serif;
        font-weight: 500;
        font-size: 1rem;
        color: #1f2937;
    }

    .stImage img {
        border-radius: 8px;
        box-shadow: 0 2px 8px rgba(0, 0, 0, 0.05);
        max-width: 100%;
        height: auto;
    }

    .stCameraInput {
        width: 100% !important;
        max-width: 800px;
        margin: 0 auto;
    }

    .stCameraInput img {
        width: 100% !important;
        max-height: 500px;
        object-fit: contain;
        border-radius: 8px;
        box-shadow: 0 2px 8px rgba(0, 0, 0, 0.05);
    }

    .stSpinner {
        color: #0d9488;
    }

    .badge {
        display: inline-block;
        padding: 6px 12px;
        border-radius: 12px;
        font-size: 0.9rem;
        font-weight: 500;
        margin-right: 8px;
    }

    .badge-fruits { background-color: #fef2f2; color: #f87171; }
    .badge-beverages { background-color: #eff6ff; color: #60a5fa; }
    .badge-snacks { background-color: #fefce8; color: #facc15; }
    .badge-dairy { background-color: #f0fdf4; color: #4ade80; }
    .badge-other { background-color: #f3f4f6; color: #9ca3af; }

    .metrics-card {
        background: #ffffff;
        border-radius: 8px;
        padding: 1.5rem;
        box-shadow: 0 2px 8px rgba(0, 0, 0, 0.05);
        margin: 1rem 0;
        border: 1px solid #e5e7eb;
        text-align: center;
    }

    .metrics-card h3 {
        font-family: 'Inter', sans-serif;
        font-weight: 500;
        font-size: 1.2rem;
        color: #1f2937;
        margin-bottom: 0.5rem;
    }

    .metrics-card p {
        font-family: 'Inter', sans-serif;
        font-weight: 700;
        font-size: 1.8rem;
        color: #111827;
    }

    .delete-button {
        background: linear-gradient(to right, #f87171, #dc2626);
        color: #ffffff;
        border: none;
        padding: 8px 16px;
        border-radius: 6px;
        font-family: 'Inter', sans-serif;
        font-weight: 600;
        font-size: 0.9rem;
        transition: all 0.3s ease;
        cursor: pointer;
    }

    .delete-button:hover {
        background: linear-gradient(to right, #ef4444, #b91c1c);
        color: #ffffff;
        transform: translateY(-1px);
        box-shadow: 0 4px 8px rgba(0, 0, 0, 0.15);
    }

    .save-button {
        background: linear-gradient(to right, #4ade80, #16a34a);
        color: #ffffff;
        border: none;
        padding: 12px 24px;
        border-radius: 6px;
        font-family: 'Inter', sans-serif;
        font-weight: 600;
        font-size: 1rem;
        transition: all 0.3s ease;
        width: 100%;
        box-shadow: 0 2px 4px rgba(0, 0, 0, 0.1);
    }

    .save-button:hover {
        background: linear-gradient(to right, #22c55e, #15803d);
        color: #ffffff;
        transform: translateY(-2px);
        box-shadow: 0 4px 8px rgba(0, 0, 0, 0.15);
    }

    .download-button {
        background: linear-gradient(to right, #6b7280, #4b5563);
        color: #ffffff;
        border: none;
        padding: 12px 24px;
        border-radius: 6px;
        font-family: 'Inter', sans-serif;
        font-weight: 600;
        font-size: 1rem;
        transition: all 0.3s ease;
        width: 100%;
        box-shadow: 0 2px 4px rgba(0, 0, 0, 0.1);
    }

    .download-button:hover {
        background: linear-gradient(to right, #4b5563, #374151);
        color: #ffffff;
        transform: translateY(-2px);
        box-shadow: 0 4px 8px rgba(0, 0, 0, 0.15);
    }

    .instruction-box {
        background: #f9fafb;
        border-radius: 8px;
        padding: 1.5rem;
        margin: 1.5rem 0;
        border-left: 4px solid;
        box-shadow: 0 2px 4px rgba(0, 0, 0, 0.05);
    }

    .instruction-box-camera {
        border-left-color: #0d9488;
    }

    .instruction-box-upload {
        border-left-color: #16a34a;
    }

    .instruction-box ol {
        padding-left: 1.5rem;
        margin-bottom: 0;
    }

    .instruction-box li {
        font-family: 'Inter', sans-serif;
        font-weight: 400;
        font-size: 1rem;
        color: #4b5563;
        margin-bottom: 0.75rem;
    }

    .sidebar .stSelectbox, .sidebar .stSlider {
        margin-bottom: 1.5rem;
    }

    .sidebar h3 {
        font-family: 'Inter', sans-serif;
        font-weight: 600;
        font-size: 1.3rem;
        color: #1f2937;
        margin-bottom: 1rem;
    }

    .sidebar p {
        font-size: 0.95rem;
        color: #6b7280;
        line-height: 1.5;
    }

    @media (max-width: 768px) {
        .stApp {
            padding: 1.5rem;
        }
        h1 {
            font-size: 2.5rem;
        }
        h2 {
            font-size: 1.6rem;
        }
        h3 {
            font-size: 1.3rem;
        }
        p, .stMarkdown, .stText {
            font-size: 0.95rem;
        }
        .stButton>button, .save-button, .download-button {
            padding: 10px 20px;
            font-size: 0.95rem;
        }
        .stCameraInput img {
            max-height: 300px;
        }
        .metrics-card p {
            font-size: 1.5rem;
        }
        .stTabs [role="tablist"] {
            flex-direction: column;
            gap: 0.5rem;
        }
        .stTabs [role="tab"] {
            padding: 0.5rem 1rem;
            font-size: 0.95rem;
        }
        .stColumns > div {
            flex-direction: column;
            gap: 1rem;
        }
        .stColumns > div > div {
            width: 100% !important;
        }
        .sidebar h3 {
            font-size: 1.2rem;
        }
        .sidebar p {
            font-size: 0.9rem;
        }
    }
</style>
""", unsafe_allow_html=True)

# Ensure results directory exists
RESULTS_DIR = Path("results")
RESULTS_DIR.mkdir(exist_ok=True)

# Global model and class variables
models = {}
model_class_files = {
    "280.pt": "classes.txt",
    "maggie.pt": "maggie.txt"
}
class_names = {}

# Initialize session state
if 'corrected_detections' not in st.session_state:
    st.session_state.corrected_detections = []
if 'processed_image' not in st.session_state:
    st.session_state.processed_image = None
if 'original_image' not in st.session_state:
    st.session_state.original_image = None
if 'use_cuda' not in st.session_state:
    st.session_state.use_cuda = True

# Function to check CUDA setup
def check_cuda_setup():
    try:
        cuda_available = torch.cuda.is_available()
        if not cuda_available:
            logger.warning("CUDA is not available. Falling back to CPU.")
            st.warning("CUDA is not available on this system. The app will run on CPU, which may be slower.")
            return False
        
        cuda_device_count = torch.cuda.device_count()
        cuda_device_name = torch.cuda.get_device_name(0) if cuda_device_count > 0 else "Unknown"
        cuda_version = torch.version.cuda if hasattr(torch.version, 'cuda') else "Unknown"
        logger.info(f"CUDA Available: {cuda_available}, Device Count: {cuda_device_count}, Device Name: {cuda_device_name}, CUDA Version: {cuda_version}")
        st.info(f"CUDA Setup: {cuda_device_count} GPU(s) detected ({cuda_device_name}, CUDA Version: {cuda_version}). Running inference on GPU.")
        return True
    except Exception as e:
        logger.error(f"Error checking CUDA setup: {str(e)}")
        st.warning(f"Error checking CUDA setup: {str(e)}. Falling back to CPU.")
        return False

# Function to categorize products and assign colors
def categorize_product(label):
    fruits = ["apple", "banana", "orange", "fruits"]
    beverages = ["water bottle", "soda", "milk", "juice"]
    snacks = ["chips", "cookies", "candy", "snacks", "cereal"]
    dairy = ["cheese", "yogurt", "eggs"]
    
    if label.lower() in [x.lower() for x in fruits]:
        return "Fruits", "badge-fruits", (248, 113, 113)  # Coral Red
    elif label.lower() in [x.lower() for x in beverages]:
        return "Beverages", "badge-beverages", (96, 165, 250)  # Sky Blue
    elif label.lower() in [x.lower() for x in snacks]:
        return "Snacks", "badge-snacks", (250, 204, 21)  # Sunflower Yellow
    elif label.lower() in [x.lower() for x in dairy]:
        return "Dairy", "badge-dairy", (74, 222, 128)  # Lime Green
    else:
        return "Other", "badge-other", (156, 163, 175)  # Soft Gray

# Function to load class names for a specific model
def load_classes(classes_file):
    try:
        if not os.path.exists(classes_file):
            default_classes = [
                "apple", "banana", "orange", "water bottle", "soda", "milk", "bread", 
                "cereal", "chips", "cookies", "pasta", "rice", "vegetables", "fruits",
                "meat", "cheese", "yogurt", "eggs", "juice", "candy", "snacks"
            ]
            with open(classes_file, 'w') as f:
                for cls in default_classes:
                    f.write(f"{cls}\n")
            logger.info(f"Created default classes file {classes_file} with {len(default_classes)} classes")
            return default_classes
        
        with open(classes_file, 'r') as f:
            classes = [line.strip() for line in f.readlines() if line.strip()]
        logger.info(f"Loaded {len(classes)} classes from {classes_file}")
        return classes
    except Exception as e:
        logger.error(f"Error loading classes file {classes_file}: {str(e)}")
        st.error(f"Failed to load classes: {str(e)}. Please ensure the classes file is accessible.")
        return ["product", "food", "beverage", "container", "package"]

# Function to initialize models
def init_models(model_paths=["280.pt", "maggie.pt"], conf_thres=0.60):
    global models, class_names
    try:
        for model_path in model_paths:
            if not os.path.exists(model_path):
                st.error(f"❌ Model file {model_path} not found. Please place it in the app directory and try again.")
                logger.error(f"Model file {model_path} not found")
                return False
                
        torch.serialization.add_safe_globals([DetectionModel])
        
        for model_path in model_paths:
            model = YOLO(model_path)
            model.conf = conf_thres
            models[model_path] = model
            class_file = model_class_files.get(model_path, "classes.txt")
            class_names[model_path] = load_classes(class_file)
            logger.info(f"Model {model_path} initialized with confidence threshold {conf_thres} and class file {class_file}")
        
        return True
    except Exception as e:
        logger.error(f"Failed to initialize models: {str(e)}")
        st.error(f"❌ Model initialization failed: {str(e)}. Please check the model files and try again.")
        models = {}
        return False

# Function to preprocess image
def preprocess_image(img):
    try:
        lab = cv2.cvtColor(img, cv2.COLOR_BGR2LAB)
        l, a, b = cv2.split(lab)
        clahe = cv2.createCLAHE(clipLimit=2.0, tileGridSize=(8, 8))
        l = clahe.apply(l)
        lab = cv2.merge((l, a, b))
        img = cv2.cvtColor(lab, cv2.COLOR_LAB2BGR)
        
        kernel = np.array([[0, -1, 0], [-1, 5, -1], [0, -1, 0]])
        img = cv2.filter2D(img, -1, kernel)
        
        logger.info("Image preprocessing completed: contrast adjustment and sharpening applied")
        return img
    except Exception as e:
        logger.error(f"Error in image preprocessing: {str(e)}")
        return img

# Draw bounding boxes
def draw_boxes(frame, boxes, confidences, class_ids, selected_model):
    detected_objects = []
    
    img_height, img_width = frame.shape[:2]
    min_dimension = min(img_height, img_width)
    box_thickness = max(3, int(min_dimension / 200))
    font_scale = max(0.8, min_dimension / 800)
    shadow_offset = max(2, int(min_dimension / 500))
    
    model_classes = class_names.get(selected_model, ["unknown"])
    
    for i, box in enumerate(boxes):
        x1, y1, x2, y2 = map(int, box)
        conf = float(confidences[i])
        label_idx = int(class_ids[i])
        
        if label_idx >= len(model_classes):
            logger.warning(f"Invalid class index {label_idx} for model {selected_model}, using default 'unknown'")
            label = "unknown"
            category, badge_class, color = "Other", "badge-other", (156, 163, 175)
        else:
            label = model_classes[label_idx]
            category, badge_class, color = categorize_product(label)
        
        cv2.rectangle(frame, (x1 + shadow_offset, y1 + shadow_offset), 
                     (x2 + shadow_offset, y2 + shadow_offset), 
                     (50, 50, 50), box_thickness)
        cv2.rectangle(frame, (x1, y1), (x2, y2), color, box_thickness)
        
        text = f"{label}: {conf:.2f}"
        text_size = cv2.getTextSize(text, cv2.FONT_HERSHEY_SIMPLEX, font_scale, 3)[0]
        
        cv2.rectangle(frame, 
                     (x1, y1 - text_size[1] - 15), 
                     (x1 + text_size[0] + 15, y1), 
                     color, -1)
        cv2.rectangle(frame, 
                     (x1 + shadow_offset, y1 - text_size[1] - 15 + shadow_offset), 
                     (x1 + text_size[0] + 15 + shadow_offset, y1 + shadow_offset), 
                     (50, 50, 50), -1)
        
        cv2.putText(frame, text, (x1 + 8, y1 - 10), 
                   cv2.FONT_HERSHEY_SIMPLEX, font_scale, (0, 0, 0), 4)
        cv2.putText(frame, text, (x1 + 8, y1 - 10), 
                   cv2.FONT_HERSHEY_SIMPLEX, font_scale, (255, 255, 255), 2)
        
        detected_objects.append({
            "label": label,
            "confidence": round(conf, 2),
            "bbox": [x1, y1, x2, y2],
            "category": category
        })
    
    logger.info(f"Drew {len(detected_objects)} bounding boxes")
    return frame, detected_objects

# Process a single image with CPU fallback
def process_image(image_data, selected_model, conf_thres=0.35):
    global models, class_names
    
    if not models or selected_model not in models:
        logger.info("Models not initialized or invalid model selected, attempting to initialize")
        if not init_models():
            raise ValueError("Model initialization failed. Please check the model files.")
    
    try:
        if isinstance(image_data, np.ndarray):
            img = image_data
        else:
            nparr = np.frombuffer(image_data, np.uint8)
            img = cv2.imdecode(nparr, cv2.IMREAD_COLOR)
        
        if img is None:
            logger.error("Failed to decode image data")
            raise ValueError("Invalid image data")
        
        logger.info(f"Processing image of size {img.shape}")
        original_size = img.shape[:2]
        
        st.session_state.original_image = cv2.cvtColor(img.copy(), cv2.COLOR_BGR2RGB)
        
        img = preprocess_image(img)
        
        max_dim = 2560
        h, w = original_size
        
        if h > w:
            new_h = int(max_dim * h / w)
            new_w = max_dim
        else:
            new_w = int(max_dim * w / h)
            new_h = max_dim
            
        process_size = (new_h, new_w)
        img_resized = cv2.resize(img, (process_size[1], process_size[0]))
        
        device = "cuda" if st.session_state.use_cuda and torch.cuda.is_available() else "cpu"
        logger.info(f"Running inference on device: {device}")
        
        # Ensure model is on the correct device
        models[selected_model].to(device)
        
        try:
            results = models[selected_model](
                img_resized,
                conf=conf_thres,
                iou=0.45,  # Default IOU threshold
                augment=True,
                verbose=False,
                max_det=100,
                device=device
            )
        except RuntimeError as e:
            if "CUDA" in str(e) and st.session_state.use_cuda:
                logger.warning(f"CUDA error during inference: {str(e)}. Falling back to CPU.")
                st.warning("CUDA error encountered during inference. Falling back to CPU for this operation.")
                st.session_state.use_cuda = False
                device = "cpu"
                models[selected_model].to(device)
                results = models[selected_model](
                    img_resized,
                    conf=conf_thres,
                    iou=0.45,
                    augment=True,
                    verbose=False,
                    max_det=100,
                    device=device
                )
            else:
                raise
        
        result = results[0]
        boxes = result.boxes.xyxy.cpu().numpy()
        confidences = result.boxes.conf.cpu().numpy()
        class_ids = result.boxes.cls.cpu().numpy()
        
        if len(boxes) > 0:
            indices = cv2.dnn.NMSBoxes(
                bboxes=boxes.tolist(),
                scores=confidences.tolist(),
                score_threshold=conf_thres,
                nms_threshold=0.45
            )
            if len(indices) > 0:
                indices = indices.flatten()
                boxes = boxes[indices]
                confidences = confidences[indices]
                class_ids = class_ids[indices]
            else:
                boxes = np.array([])
                confidences = np.array([])
                class_ids = np.array([])
        
        if boxes.shape[0] > 0:
            scale_x = original_size[1] / process_size[1]
            scale_y = original_size[0] / process_size[0]
            boxes[:, 0] *= scale_x
            boxes[:, 1] *= scale_y
            boxes[:, 2] *= scale_x
            boxes[:, 3] *= scale_y
        
        processed_img, detected_objects = draw_boxes(img.copy(), boxes, confidences, class_ids, selected_model)
        st.session_state.processed_image = processed_img
        
        logger.info(f"Image processed successfully with {selected_model}, detected {len(detected_objects)} objects on {device}")
        return processed_img, detected_objects
        
    except Exception as e:
        cuda_debug_msg = """
        **CUDA Debugging Steps:**
        1. Set the environment variable `CUDA_LAUNCH_BLOCKING=1` before running the app:
           - On Linux/Mac: `export CUDA_LAUNCH_BLOCKING=1`
           - On Windows: `set CUDA_LAUNCH_BLOCKING=1`
        2. Enable device-side assertions by setting `TORCH_USE_CUDA_DSA=1`:
           - On Linux/Mac: `export TORCH_USE_CUDA_DSA=1`
           - On Windows: `set TORCH_USE_CUDA_DSA=1`
        3. Check your CUDA setup:
           - Ensure NVIDIA drivers are up to date.
           - Verify PyTorch CUDA version matches your CUDA toolkit (run `print(torch.version.cuda)`).
           - Check GPU memory usage (`nvidia-smi`) and ensure there's enough free memory.
        4. If the issue persists, try running the app with CPU only by setting `use_cuda` to False in the app settings.
        """
        error_msg = f"Image processing error: {str(e)}."
        if "CUDA" in str(e):
            error_msg += cuda_debug_msg
        logger.error(error_msg)
        raise ValueError(error_msg)

# Function to create category visualization
def create_category_visualization(detected_objects):
    if not detected_objects:
        return None
    
    categories = [obj["category"] for obj in detected_objects]
    category_counts = pd.Series(categories).value_counts()
    
    plt.figure(figsize=(8, 5))
    colors = {
        "Fruits": "#f87171",
        "Beverages": "#60a5fa",
        "Snacks": "#facc15",
        "Dairy": "#4ade80",
        "Other": "#9ca3af"
    }
    category_counts.plot(kind="bar", color=[colors.get(cat, "#9ca3af") for cat in category_counts.index])
    plt.title("Product Category Distribution", fontsize=14, pad=15)
    plt.xlabel("Category", fontsize=12)
    plt.ylabel("Count", fontsize=12)
    plt.xticks(rotation=45)
    plt.tight_layout()
    
    buf = io.BytesIO()
    plt.savefig(buf, format="png")
    buf.seek(0)
    plt.close()
    return buf

# Function to display and edit detections
def display_and_edit_detections(detected_objects, key_prefix):
    if not detected_objects:
        return detected_objects

    if f"{key_prefix}_edited_detections" not in st.session_state:
        st.session_state[f"{key_prefix}_edited_detections"] = [
            {
                "label": obj["label"], 
                "confidence": obj["confidence"], 
                "category": obj["category"], 
                "category_badge": categorize_product(obj["label"])[1]
            }
            for obj in detected_objects
        ]

    edited_detections = st.session_state[f"{key_prefix}_edited_detections"]

    st.subheader("Edit Detected Products")
    for i, detection in enumerate(edited_detections):
        with st.container():
            col1, col2, col3 = st.columns([3, 2, 1])
            with col1:
                new_label = st.text_input(f"Product Name {i}", value=detection["label"], key=f"{key_prefix}_label_{i}")
                if new_label != detection["label"]:
                    edited_detections[i]["label"] = new_label
                    edited_detections[i]["category"], edited_detections[i]["category_badge"] = categorize_product(new_label)[:2]
            with col2:
                st.markdown(f'<span class="badge {detection["category_badge"]}">{detection["category"]}</span>', unsafe_allow_html=True)
            with col3:
                if st.button("Delete", key=f"{key_prefix}_delete_{i}", help="Remove this detection"):
                    edited_detections.pop(i)
                    st.session_state[f"{key_prefix}_edited_detections"] = edited_detections
                    st.experimental_rerun()

    if st.button("Save Corrections", key=f"{key_prefix}_save", help="Save changes to persist throughout the session", type="primary"):
        st.session_state.corrected_detections = [
            {"label": det["label"], "confidence": det["confidence"], "category": det["category"], "category_badge": det["category_badge"]}
            for det in edited_detections
        ]
        st.success("Corrections saved successfully!")

    df_data = [
        {
            "Product": det["label"],
            "Category": f'<span class="badge {det["category_badge"]}">{det["category"]}</span>',
            "Confidence": f"{det['confidence']*100:.1f}%"
        }
        for det in edited_detections
    ]
    df = pd.DataFrame(df_data)

    st.subheader("Detection Statistics")
    col1, col2, col3 = st.columns(3)
    with col1:
        st.markdown('<div class="metrics-card"><h3>Total Detections</h3><p>{}</p></div>'.format(len(edited_detections)), unsafe_allow_html=True)
    with col2:
        unique_categories = len(set(det["category"] for det in edited_detections))
        st.markdown('<div class="metrics-card"><h3>Categories Detected</h3><p>{}</p></div>'.format(unique_categories), unsafe_allow_html=True)
    with col3:
        avg_conf = np.mean([det["confidence"] for det in edited_detections]) * 100 if edited_detections else 0
        st.markdown('<div class="metrics-card"><h3>Avg. Confidence</h3><p>{:.1f}%</p></div>'.format(avg_conf), unsafe_allow_html=True)

    st.markdown(df.to_html(escape=False), unsafe_allow_html=True)

    st.subheader("Category Distribution")
    viz_buf = create_category_visualization(edited_detections)
    if viz_buf:
        st.image(viz_buf, caption="Distribution of Detected Product Categories", use_container_width
=True)
    else:
        st.info("No detections to visualize.")

    return edited_detections

# Streamlit app
def main():
    st.title("📦 Grocery Product Detection")
    st.markdown("Identify grocery products with precision using advanced YOLO models.")
    
    # Check CUDA setup at startup
    if 'cuda_checked' not in st.session_state:
        st.session_state.use_cuda = check_cuda_setup()
        st.session_state.cuda_checked = True

    # Sidebar (IOU Threshold removed)
    with st.sidebar:
        st.markdown('<div class="sidebar">', unsafe_allow_html=True)
        st.header("⚙️ Settings")
        
        model_options = ["280.pt", "maggie.pt"]
        selected_model = st.selectbox("Select Detection Model", model_options, key="model_select")
        
        conf_thres = st.slider(
            "Confidence Threshold",
            min_value=0.1,
            max_value=0.9,
            value=0.35,
            step=0.05,
            key="sidebar_conf",
            help="Minimum confidence for detections"
        )

        # Option to toggle CUDA usage
        st.session_state.use_cuda = st.checkbox(
            "Use GPU (CUDA)",
            value=st.session_state.use_cuda,
            key="use_cuda_checkbox",
            help="Uncheck to force CPU usage if CUDA issues persist."
        )
        
        st.header("💡 Detection Tips")
        st.markdown("""
        - **Lighting**: Ensure good lighting for clear images.
        - **Positioning**: Place items centrally and avoid overlap.
        - **Resolution**: Higher resolutions improve accuracy but may slow processing.
        - **Confidence**: Lower confidence for more detections.
        """)
        st.markdown('</div>', unsafe_allow_html=True)

    # Initialize models
    if 'models_initialized' not in st.session_state:
        with st.spinner("Loading models..."):
            st.session_state.models_initialized = init_models()

    # Tabs
    tab1, tab2 = st.tabs(["Camera Detection", "Image Upload"])

    with tab1:
        st.subheader("Camera Detection")
        st.markdown('<div class="camera-section">', unsafe_allow_html=True)
        
        resolution_options = ["HD (1280x720)", "Full HD (1920x1080)", "Custom"]
        selected_resolution = st.selectbox("Select Camera Resolution", resolution_options, key="camera_resolution")
        
        custom_width, custom_height = 1280, 720
        if selected_resolution == "Custom":
            col1, col2 = st.columns(2)
            with col1:
                custom_width = st.number_input("Width (px)", min_value=320, max_value=3840, value=1280, step=10, key="custom_width")
            with col2:
                custom_height = st.number_input("Height (px)", min_value=240, max_value=2160, value=720, step=10, key="custom_height")
        
        if selected_resolution == "HD (1280x720)":
            camera_width, camera_height = 1280, 720
        elif selected_resolution == "Full HD (1920x1080)":
            camera_width, camera_height = 1920, 1080
        else:
            camera_width, camera_height = custom_width, custom_height

        st.markdown("""
        <div class="instruction-box instruction-box-camera">
        <h3>How to Use the Camera</h3>
        <ol>
            <li>Select the desired camera resolution above.</li>
            <li>Adjust confidence threshold in the sidebar for optimal detection.</li>
            <li>Click the "Capture Image" button below.</li>
            <li>Grant camera permissions when prompted.</li>
            <li>Position grocery items clearly in the frame.</li>
            <li>Capture the photo to detect products automatically.</li>
        </ol>
        </div>
        """, unsafe_allow_html=True)
        
        camera_input = st.camera_input("Capture Image", key="camera")
        
        if camera_input:
            try:
                with st.spinner("Processing image..."):
                    img_bytes = camera_input.getvalue()
                    processed_img, detected_objects = process_image(img_bytes, selected_model, conf_thres=conf_thres)
                
                processed_img_rgb = cv2.cvtColor(processed_img, cv2.COLOR_BGR2RGB)
                
                st.subheader("Image Comparison")
                col1, col2 = st.columns(2)
                with col1:
                    if st.session_state.original_image is not None:
                        st.image(st.session_state.original_image, caption="Original Image", use_container_width
=True)
                    else:
                        st.info("Original image not available.")
                with col2:
                    st.image(processed_img_rgb, caption=f"Processed Image (Model: {selected_model})", use_container_width
=True)
                
                _, img_buf = cv2.imencode(".png", processed_img)
                img_b64 = b64encode(img_buf).decode()
                href = f'<a href="data:image/png;base64,{img_b64}" download="processed_image.png" class="download-button">Download Processed Image</a>'
                st.markdown(href, unsafe_allow_html=True)
                
                if detected_objects:
                    st.success(f"Detected {len(detected_objects)} product{'s' if len(detected_objects) > 1 else ''}")
                    
                    st.markdown('<div class="result-container">', unsafe_allow_html=True)
                    detected_objects = display_and_edit_detections(detected_objects, "camera")
                    st.markdown('</div>', unsafe_allow_html=True)
                    
                    filename = f"{uuid.uuid4()}.jpg"
                    result_path = RESULTS_DIR / filename
                    cv2.imwrite(str(result_path), processed_img)
                else:
                    st.warning("No products detected. Try adjusting the confidence threshold in the sidebar or taking a clearer photo.")
            
            except ValueError as e:
                st.error(f"{str(e)}")
            except Exception as e:
                st.error(f"Error processing image: {str(e)}. Please try again or check the logs for details.")
                logger.error(f"Camera processing error: {str(e)}")
        
        st.markdown('</div>', unsafe_allow_html=True)
        st.divider()
        
        with st.expander("Camera Troubleshooting Guide"):
            st.markdown("""
            ### Camera Not Working? Try These Steps
            <ol>
                <li><b>Check Permissions</b>: Ensure camera access is granted in browser settings.</li>
                <li><b>Browser Compatibility</b>: Use Chrome, Firefox, or Safari for best support.</li>
                <li><b>Device Camera</b>: Test your camera with another app to confirm it works.</li>
                <li><b>Lighting</b>: Ensure adequate lighting for better detection.</li>
                <li><b>Resolution</b>: Lower resolution if the camera is slow (e.g., HD instead of Full HD).</li>
                <li><b>Refresh</b>: Refresh the browser to reset camera initialization.</li>
                <li><b>Alternative</b>: Use the "Image Upload" tab if issues persist.</li>
                <li><b>Logs</b>: Check browser console (F12) for error messages.</li>
            </ol>
            """, unsafe_allow_html=True)
    
    with tab2:
        st.subheader("Image Upload Detection")
        st.markdown('<div class="result-container">', unsafe_allow_html=True)
        
        st.markdown("""
        <div class="instruction-box instruction-box-upload">
        <h3>How to Upload an Image</h3>
        <ol>
            <li>Adjust confidence threshold in the sidebar for optimal detection.</li>
            <li>Click the "Browse files" button below to select an image (JPG/PNG).</li>
            <li>Ensure the image is clear and well-lit for best results.</li>
            <li>Upload the image to detect products automatically.</li>
        </ol>
        </div>
        """, unsafe_allow_html=True)
        
        uploaded_file = st.file_uploader(
            "Upload an image (JPG, PNG)", 
            type=["jpg", "jpeg", "png"],
            key="uploader"
        )
        
        if uploaded_file is not None:
            try:
                if uploaded_file.size > 10 * 1024 * 1024:
                    st.error("File size exceeds 10MB limit. Please upload a smaller image.")
                    logger.error("File size exceeds 10MB")
                else:
                    img_data = uploaded_file.read()
                    
                    with st.spinner("Processing image..."):
                        processed_img, detected_objects = process_image(img_data, selected_model, conf_thres=conf_thres)
                    
                    filename = f"{uuid.uuid4()}.jpg"
                    result_path = RESULTS_DIR / filename
                    cv2.imwrite(str(result_path), processed_img)
                    
                    processed_img_rgb = cv2.cvtColor(processed_img, cv2.COLOR_BGR2RGB)
                    
                    st.subheader("Image Comparison")
                    col1, col2 = st.columns(2)
                    with col1:
                        if st.session_state.original_image is not None:
                            st.image(st.session_state.original_image, caption="Original Image", use_container_width
=True)
                        else:
                            st.info("Original image not available.")
                    with col2:
                        st.image(processed_img_rgb, caption=f"Processed Image (Model: {selected_model})", use_container_width
=True)
                    
                    _, img_buf = cv2.imencode(".png", processed_img)
                    img_b64 = b64encode(img_buf).decode()
                    href = f'<a href="data:image/png;base64,{img_b64}" download="processed_image.png" class="download-button">Download Processed Image</a>'
                    st.markdown(href, unsafe_allow_html=True)
                    
                    if detected_objects:
                        st.success(f"Detected {len(detected_objects)} product{'s' if len(detected_objects) > 1 else ''}")
                        
                        detected_objects = display_and_edit_detections(detected_objects, "upload")
                    
                    else:
                        st.warning("No products detected. Try adjusting the confidence threshold in the sidebar or using a clearer image.")
                    
                    logger.info(f"Image detection completed with {selected_model}, saved as {filename}")
                
            except ValueError as e:
                st.error(f"{str(e)}")
            except Exception as e:
                st.error(f"Error processing image: {str(e)}. Please try a different image or check the logs.")
                logger.error(f"Unexpected error in image upload: {str(e)}")
        
        st.markdown('</div>', unsafe_allow_html=True)
        st.divider()

if __name__ == "__main__":
    main()