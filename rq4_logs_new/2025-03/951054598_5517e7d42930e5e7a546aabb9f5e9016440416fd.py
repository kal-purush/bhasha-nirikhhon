import os
import numpy as np
import tensorflow as tf
from flask import Flask, request, render_template, jsonify
from werkzeug.utils import secure_filename
from flask_cors import CORS
from tensorflow.keras.applications import MobileNetV2 # type: ignore # type: ignore
from tensorflow.keras.applications.mobilenet_v2 import preprocess_input # type: ignore
from tensorflow.keras.applications.imagenet_utils import decode_predictions # type: ignore
from tensorflow.keras.preprocessing import image # type: ignore

# Force TensorFlow to use CPU (Prevents GPU errors)
os.environ["CUDA_VISIBLE_DEVICES"] = "-1"

# Initialize Flask app
app = Flask(__name__)
CORS(app)  # Enable CORS

# Set upload folder
UPLOAD_FOLDER = "uploads"
ALLOWED_EXTENSIONS = {"png", "jpg", "jpeg"}
app.config["UPLOAD_FOLDER"] = UPLOAD_FOLDER

# Ensure upload directory exists
if not os.path.exists(UPLOAD_FOLDER):
    os.makedirs(UPLOAD_FOLDER)

# Load MobileNetV2 Model
model = MobileNetV2(weights="imagenet")

# Function to check allowed file types
def allowed_file(filename):
    return "." in filename and filename.rsplit(".", 1)[1].lower() in ALLOWED_EXTENSIONS

# Fashion recommendations dictionary
recommendations = {
    "jean": {"outfit": "Casual Denim", "colors": ["Blue", "Black"], "accessories": ["Sneakers", "Cap"]},
    "suit": {"outfit": "Formal Business", "colors": ["Navy Blue", "Gray"], "accessories": ["Tie", "Leather Shoes"]},
    "dress": {"outfit": "Elegant Evening Wear", "colors": ["Red", "Black"], "accessories": ["Earrings", "Heels"]},
    "jacket": {"outfit": "Streetwear", "colors": ["Black", "White"], "accessories": ["Sunglasses", "Backpack"]},
    "shirt": {"outfit": "Smart Casual", "colors": ["White", "Beige"], "accessories": ["Watch", "Loafers"]},
    "t-shirt": {"outfit": "Casual Everyday", "colors": ["Gray", "Blue"], "accessories": ["Cap", "Sneakers"]},
    "blouse": {"outfit": "Elegant Chic", "colors": ["Pink", "White"], "accessories": ["Bracelet", "Heels"]},
    "trousers": {"outfit": "Business Casual", "colors": ["Beige", "Navy"], "accessories": ["Belt", "Loafers"]},
    "coat": {"outfit": "Winter Formal", "colors": ["Brown", "Gray"], "accessories": ["Scarf", "Boots"]},
    "hoodie": {"outfit": "Urban Casual", "colors": ["Black", "Gray"], "accessories": ["Cap", "Sneakers"]},
    "sweater": {"outfit": "Cozy Winter", "colors": ["Cream", "Brown"], "accessories": ["Scarf", "Boots"]},
    "skirt": {"outfit": "Chic Look", "colors": ["Pastel", "Black"], "accessories": ["Necklace", "Flats"]},
    "shorts": {"outfit": "Summer Casual", "colors": ["Light Blue", "White"], "accessories": ["Sunglasses", "Sandals"]},
    "default": {"outfit": "Trendy Look", "colors": ["White", "Black"], "accessories": ["Minimal Jewelry", "Casual Shoes"]},
}

# Function to get outfit recommendation
def get_fashion_recommendation(img_path):
    try:
        # Load and preprocess the image
        img = image.load_img(img_path, target_size=(224, 224))
        img_array = image.img_to_array(img)
        img_array = np.expand_dims(img_array, axis=0)
        img_array = preprocess_input(img_array)

        # Get AI Predictions
        predictions = model.predict(img_array)
        decoded_predictions = decode_predictions(predictions, top=5)[0]  # Get top 5 predictions

        # Extract detected objects
        detected_objects = [pred[1].lower() for pred in decoded_predictions]

        print("\n🔍 Detected Objects:", detected_objects)  # Debugging: See what AI detects

        # Try to match detected objects with recommendations
        for obj in detected_objects:
            for key in recommendations.keys():
                if key in obj:  # Flexible match (e.g., "t-shirt" matches "shirt")
                    return {"detected": detected_objects, "recommendation": recommendations[key]}

        # If no match, return default recommendation
        return {"detected": detected_objects, "recommendation": recommendations["default"]}

    except Exception as e:
        return {"error": str(e)}

# Homepage Route
@app.route("/")
def home():
    return render_template("index1.html")

# Upload Image Route
@app.route("/upload", methods=["POST"])
def upload_image():
    if "file" not in request.files:
        return jsonify({"error": "No file part"})

    file = request.files["file"]

    if file.filename == "":
        return jsonify({"error": "No selected file"})

    if file and allowed_file(file.filename):
        filename = secure_filename(file.filename)
        file_path = os.path.join(app.config["UPLOAD_FOLDER"], filename)
        file.save(file_path)

        # Get AI-based fashion recommendations
        recommendations_result = get_fashion_recommendation(file_path)

        return jsonify({
            "message": "Image processed successfully",
            "file_path": file_path,
            "detected_objects": recommendations_result["detected"],  # Shows what AI detected
            "recommendations": recommendations_result["recommendation"]
        })

    return jsonify({"error": "Invalid file type"})

if __name__ == "__main__":
    app.run(debug=True, port=5000)