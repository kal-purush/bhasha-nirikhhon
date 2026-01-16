import os
import shutil
import logging #To log the computation process
import xml.etree.ElementTree as ET #To parse anotations which are in xml formats
from sklearn.model_selection import train_test_split # To split the data into train and test pieces
from ultralytics import YOLO # YOLO model for training

# Configure logging -> output format 
logging.basicConfig(level=logging.INFO, format="%(asctime)s - %(levelname)s - %(message)s")
def log_and_print(message):
    logging.info(message)
    print(message)


base_path = r"D:\HIWI\p15"
images_base_path = os.path.join(base_path, "Images")
annotations_base_path = os.path.join(base_path, "Annotation")
yolo_dataset_path = os.path.join(base_path, "YOLO_Dataset")
images_dir = os.path.join(yolo_dataset_path, "images")
labels_dir = os.path.join(yolo_dataset_path, "labels")

# Class mapping (Key/value) since the YOLO model requires numeric labels for classes
class_mapping = {"missing_hole": 0, "open_circuit": 1}

# Ensure YOLO dataset directories exist
os.makedirs(os.path.join(images_dir, "train"), exist_ok=True) 
os.makedirs(os.path.join(images_dir, "val"), exist_ok=True) #For validation set
os.makedirs(os.path.join(labels_dir, "train"), exist_ok=True)
os.makedirs(os.path.join(labels_dir, "val"), exist_ok=True) #For validation set


#YOLO annotation format: <class_id> <x_center> <y_center> <width> <height>
#Note that the classID is a numeric ID not a string ID

#region Step 1

#Helper function for Conversion
def convert_to_yolo(size, box):
    """Convert VOC box format to YOLO format."""
    dw = 1.0 / size[0]
    dh = 1.0 / size[1]
    x_center = (box[0] + box[1]) / 2.0
    y_center = (box[2] + box[3]) / 2.0
    width = box[1] - box[0]
    height = box[3] - box[2]
    return x_center * dw, y_center * dh, width * dw, height * dh
#Helper function for extracting information form xml annotation files and transforming it into YOLO compatible annotation set
def process_xml_file(xml_file, output_folder):
    """Convert XML annotations to YOLO format."""
    tree = ET.parse(xml_file)
    root = tree.getroot()

    # Image size
    size = root.find("size")
    width = int(size.find("width").text)
    height = int(size.find("height").text)

    # Output label file
    base_filename = os.path.splitext(root.find("filename").text)[0]
    output_file_path = os.path.join(output_folder, f"{base_filename}.txt")
    
    with open(output_file_path, "w") as output_file:
        for obj in root.iter("object"):
            class_name = obj.find("name").text
            if class_name not in class_mapping:
                log_and_print(f"Skipping unknown class: {class_name}")
                continue
            class_id = class_mapping[class_name]
            bndbox = obj.find("bndbox")
            xmin = float(bndbox.find("xmin").text)
            xmax = float(bndbox.find("xmax").text)
            ymin = float(bndbox.find("ymin").text)
            ymax = float(bndbox.find("ymax").text)
            bbox = (xmin, xmax, ymin, ymax)
            yolo_box = convert_to_yolo((width, height), bbox)
            output_file.write(f"{class_id} {' '.join(map(str, yolo_box))}\n")

def prepare_dataset():
    """Convert all XML annotations to YOLO format."""
    log_and_print("Preparing dataset...")
    for subdir in os.listdir(annotations_base_path):
        subdir_path = os.path.join(annotations_base_path, subdir)
        if os.path.isdir(subdir_path):
            for file_name in os.listdir(subdir_path):
                if file_name.endswith(".xml"):
                    process_xml_file(os.path.join(subdir_path, file_name), labels_dir)
    log_and_print("Annotations converted to YOLO format.")
    
#endregion


def validate_dataset():
    """Ensure every image has a corresponding annotation file."""
    log_and_print("Validating dataset...")
    for subdir in os.listdir(images_base_path):
        subdir_path = os.path.join(images_base_path, subdir)
        if os.path.isdir(subdir_path):
            for image_file in os.listdir(subdir_path):
                if image_file.endswith(('.jpg', '.png', '.jpeg')):
                    base_name = os.path.splitext(image_file)[0]
                    label_file = f"{base_name}.txt"
                    if not os.path.exists(os.path.join(labels_dir, label_file)):
                        raise ValueError(f"Missing annotation for image: {image_file}")
    log_and_print("Dataset validation passed.")

def split_dataset():
    """Split images and labels into training and validation sets."""
    log_and_print("Splitting dataset into training and validation sets...")
    images = []
    for subdir in os.listdir(images_base_path):
        subdir_path = os.path.join(images_base_path, subdir)
        if os.path.isdir(subdir_path):
            images += [os.path.join(subdir, f) for f in os.listdir(subdir_path) if f.endswith(('.jpg', '.png', '.jpeg'))]

    if not images:
        raise ValueError("No images found in the Images directory. Please ensure the directory is populated with images.")

    train_images, val_images = train_test_split(images, test_size=0.2, random_state=42)

    for image in train_images:
        src_image_path = os.path.join(images_base_path, image)
        dest_image_path = os.path.join(images_dir, "train", os.path.basename(image))
        shutil.copy(src_image_path, dest_image_path)
        label_file = os.path.splitext(os.path.basename(image))[0] + ".txt"
        src_label_path = os.path.join(labels_dir, label_file)
        if os.path.exists(src_label_path):
            shutil.copy(src_label_path, os.path.join(labels_dir, "train", label_file))

    for image in val_images:
        src_image_path = os.path.join(images_base_path, image)
        dest_image_path = os.path.join(images_dir, "val", os.path.basename(image))
        shutil.copy(src_image_path, dest_image_path)
        label_file = os.path.splitext(os.path.basename(image))[0] + ".txt"
        src_label_path = os.path.join(labels_dir, label_file)
        if os.path.exists(src_label_path):
            shutil.copy(src_label_path, os.path.join(labels_dir, "val", label_file))

    log_and_print("Dataset split complete.")

def create_data_yaml():
    """Create the data.yaml file for YOLO training."""
    log_and_print("Creating data.yaml file...")
    yaml_content = f"""
train: {os.path.join(images_dir, 'train')}
val: {os.path.join(images_dir, 'val')}
nc: {len(class_mapping)}  # Number of classes
names: {list(class_mapping.keys())}
"""
    with open(os.path.join(base_path, "data.yaml"), "w") as yaml_file:
        yaml_file.write(yaml_content.strip())
    log_and_print("data.yaml created.")

def train_yolo_model(epochs=20, imgsz=640, batch_size=16):
    """Train YOLOv8 using the prepared dataset."""
    log_and_print("Starting YOLO training...")
    model = YOLO("yolov8n.pt")  # Pre-trained YOLO model
    model.train(
        data=os.path.join(base_path, "data.yaml"),
        epochs=epochs,
        imgsz=imgsz,
        batch=batch_size,
    )
    log_and_print("YOLO training complete.")
    model.export(format="onnx")
    log_and_print("YOLO model exported to ONNX format.")
    # Save the trained model
    model_path = os.path.join(base_path, "yolov8n_cls_trained.pt")
    model.save(model_path)
    log_and_print(f"Trained model saved to {model_path}")

def evaluate_model():
    """Evaluate the trained YOLO model."""
    log_and_print("Evaluating YOLO model...")
    model = YOLO(os.path.join("runs/detect/train", "weights", "best.pt"))
    results = model.val()
    log_and_print(f"Evaluation results: {results}")

def save_predictions():
    """Run inference and save predictions."""
    log_and_print("Saving predictions...")
    model = YOLO(os.path.join("runs/detect/train", "weights", "best.pt"))
    results = model.predict(source=os.path.join(images_dir, "val"), save=True, imgsz=640)
    log_and_print(f"Predictions saved in {results.save_dir}")

if __name__ == "__main__":
    log_and_print("Starting PCB defect classification task.")

    # Step 1: Prepare dataset
    prepare_dataset()

    # Step 2: Validate dataset
    validate_dataset()

    # Step 3: Split dataset
    split_dataset()

    # Step 4: Create data.yaml
    create_data_yaml()

    # Step 5: Train YOLO model
    train_yolo_model(epochs=1, imgsz=640)

    # Step 6: Evaluate model
    evaluate_model()

    # Step 7: Save predictions
    save_predictions()