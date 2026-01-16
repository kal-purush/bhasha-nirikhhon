import pandas as pd
import os
from flask import Flask, render_template, request, jsonify, url_for # Import jsonify and url_for
from werkzeug.utils import secure_filename
from tensorflow.keras.preprocessing import image # type: ignore
import numpy as np
from tensorflow.keras.models import load_model # type: ignore
import google.generativeai as genai # Import genai
from datetime import datetime # Import datetime for current year in footer

app = Flask(__name__)

# --- CONFIGURATION ---
UPLOAD_FOLDER = 'static/uploads'
MODIFIED_FOLDER = 'static/modified' # This folder should contain your pre-processed images
ALLOWED_EXTENSIONS = {'png', 'jpg', 'jpeg'}
app.config['UPLOAD_FOLDER'] = UPLOAD_FOLDER # Standard Flask way to store config

# Define image dimensions (should match what you used for training)
IMG_WIDTH = 224
IMG_HEIGHT = 224

# Mapping from numerical diagnosis to category name
diagnosis_map = {
    0: 'No_DR',
    1: 'Mild',
    2: 'Moderate',
    3: 'Severe',
    4: 'Proliferate_DR'
}
reverse_diagnosis_map = {v: k for k, v in diagnosis_map.items()}
class_names = list(diagnosis_map.values()) # List of class names for prediction output

# Mapping from DR class to comprehensive text output for display (kept for model prediction display)
dr_to_text_map = {
    'No_DR': 'No Diabetics<br>Cardiac Risk Absent<br>Glaucoma Risk Absent<br>Healthy Retina Observed',
    'Mild': 'Mild Diabetics<br>Low Cardiac Risk<br>Low Glaucoma Risk<br>Few Microaneurysms Seen.',
    'Moderate': 'Moderate Diabetics<br>Cardiac Risk Seen.<br>Glaucoma Risk Seen.<br>More lesions present.',
    'Severe': 'Severe Diabetics.<br>High Cardiac Risk.<br>Presence of Glaucoma.<br>Extensive Damage Noted.',
    'Proliferate_DR': 'Proliferate Diabetics<br>High Cardiac Risk.<br>High Glaucoma Risk.<br>Neovascualrization Present.'
}

# --- END CONFIGURATION ---


# --- Model Loading ---
# Load the trained model weights on startup
MODEL_WEIGHTS_PATH = 'diabetic_retinopathy_classification_weights.weights.h5' # Ensure this file is in the same directory as app.py
loaded_model = None # Initialize model as None

def load_trained_model(weights_path):
    """Loads the pre-trained DenseNet121 model and its weights."""
    from tensorflow.keras.applications import DenseNet121 # type: ignore
    from tensorflow.keras.layers import Dense, GlobalAveragePooling2D, Dropout # type: ignore
    from tensorflow.keras.models import Model # type: ignore

    try:
        base_model = DenseNet121(weights='imagenet', include_top=False, input_shape=(IMG_WIDTH, IMG_HEIGHT, 3))
        x = base_model.output
        x = GlobalAveragePooling2D()(x)
        x = Dense(512, activation='relu')(x)
        x = Dropout(0.5)(x)
        num_classes = len(diagnosis_map)
        predictions = Dense(num_classes, activation='softmax')(x)
        model = Model(inputs=base_model.input, outputs=predictions)

        # Ensure the weights file path is absolute relative to the app file's location
        absolute_weights_path = os.path.join(app.root_path, weights_path)

        if os.path.exists(absolute_weights_path):
            model.load_weights(absolute_weights_path)
            print(f"Successfully loaded model weights from {absolute_weights_path}")
        else:
            print(f"Warning: Model weights file not found at '{absolute_weights_path}'.")
            print("Prediction will not work correctly until weights are trained and saved.")
            # Do NOT load weights if the file doesn't exist, the model will use random init

        model.compile(optimizer='adam', loss='categorical_crossentropy', metrics=['accuracy'])
        return model

    except Exception as e:
        print(f"Error loading model or weights: {e}")
        print("Please ensure TensorFlow and other libraries are installed correctly.")
        return None # Return None if model loading fails

# Attempt to load the model when the app starts
loaded_model = load_trained_model(MODEL_WEIGHTS_PATH)

# --- End Model Loading ---


# --- Gemini Chatbot Configuration ---
# Replace with your actual API key obtained from Google AI Studio or Google Cloud
# WARNING: Storing API keys directly in code is NOT recommended for production environments.
# Use environment variables (e.g., os.environ.get('GEMINI_API_KEY')) or a secrets management system.
GEMINI_API_KEY = "AIzaSyCOR-Y14uiPPNWapMyelZ9oO4xmP55YWdU" # <-- Your specific key found to be working

gemini_model = None
chat_session = None # Initialize chat_session as None globally

# Configure Gemini API only if a valid key is provided
if GEMINI_API_KEY and GEMINI_API_KEY != "YOUR_GEMINI_API_KEY":
    try:
        genai.configure(api_key=GEMINI_API_KEY)
        # Initialize the model here, but start the session on the first message or for summary
        # Using the model name that you found works
        gemini_model = genai.GenerativeModel('gemini-2.0-flash')
        print("Gemini API configured.")

    except Exception as e:
        print(f"Error configuring Gemini API. Chatbot and summary generation will be unavailable: {e}")
        gemini_model = None # Ensure model is None if configuration fails
else:
    print("Warning: GEMINI_API_KEY not set or is placeholder. Chatbot and summary generation will be unavailable.")

# --- End Gemini Chatbot Configuration ---


# --- Helper Functions ---
def allowed_file(filename):
    """Checks if the uploaded filename has an allowed extension."""
    return '.' in filename and filename.rsplit('.', 1)[1].lower() in ALLOWED_EXTENSIONS

def predict_image(image_path):
    """
    Loads and preprocesses the image, makes a prediction using the loaded model,
    and returns the comprehensive text prediction and the original image ID.
    """
    # Check if model was successfully loaded before attempting prediction
    if loaded_model is None:
        return "Model weights not loaded. Cannot make prediction.", None, None

    try:
        # Load and preprocess the image
        img = image.load_img(image_path, target_size=(IMG_WIDTH, IMG_HEIGHT))
        img_array = image.img_to_array(img)
        img_array = np.expand_dims(img_array, axis=0) # Add batch dimension
        img_array /= 255.0 # Normalize pixel values to [0, 1]

        # Make prediction
        predictions = loaded_model.predict(img_array)
        predicted_class_index = np.argmax(predictions, axis=1)[0]
        predicted_class_name = class_names[predicted_class_index]

        # Get comprehensive text (using the map for display, not for Gemini input)
        comprehensive_prediction = dr_to_text_map.get(predicted_class_name, f"Prediction: {predicted_class_name}")

        # Extract image ID from filename (without extension)
        image_id = os.path.splitext(os.path.basename(image_path))[0]

        return comprehensive_prediction, image_id, predicted_class_name # Return text, id, and class name

    except Exception as e:
        print(f"Error during prediction: {e}")
        # Return error message, None for id and class name if prediction fails
        return f"Error during prediction: {e}", None, None

def find_matching_modified_image(image_id):
    """Searches for a matching image (with .png extension) in the static/modified folder."""
    target_filename = image_id + ".png"
    # Construct the absolute path to check existence
    absolute_modified_image_path = os.path.join(app.root_path, MODIFIED_FOLDER, target_filename)

    if os.path.exists(absolute_modified_image_path):
        # Return the path relative *to the static folder*, using forward slashes.
        # MODIFIED_FOLDER is 'static/modified', we need 'modified/filename.png'
        path_relative_to_static = os.path.join('modified', target_filename).replace(os.sep, '/')
        return path_relative_to_static

    return None

def generate_result_summary(predicted_class_name):
    """
    Uses Gemini to generate a short explanation and relevant information based on the predicted DR class.
    Returns a dictionary with 'explanation' and 'precautions' or None on error.
    """
    if gemini_model is None:
        print("Gemini model not available for summary generation.")
        return None

    # Define the prompt for Gemini - requesting specific format
    prompt = f"""
    As an AI medical assistant for a Diabetic Retinopathy analysis tool, please provide:
    1. A brief explanation of the predicted diagnosis "{predicted_class_name}".
    2. Relevant general information or precautions related to this diagnosis and its potential links to cardiac and glaucoma risks, based on common medical understanding (but not medical advice).

    Format your response strictly as follows:
    EXPLANATION: [Your concise explanation here]
    PRECAUTIONS: [Your relevant information/precautions here]

    Keep both the explanation and precautions concise (1-3 sentences each).
    Reiterate that this tool is for informational purposes and not medical advice.
    """

    print(f"Sending prompt to Gemini for summary: {prompt}") # Debugging: Log the prompt

    try:
        # Use generate_content for a single turn interaction for the summary
        response = gemini_model.generate_content(prompt)
        response_text = response.text.strip()

        print(f"Received raw response from Gemini for summary:\n{response_text}") # Debugging: Log the raw response

        # Parse the response based on the strict format
        explanation = "Could not generate explanation."
        precautions = "Could not generate precautions."

        if "EXPLANATION:" in response_text and "PRECAUTIONS:" in response_text:
            try:
                parts = response_text.split("PRECAUTIONS:", 1) # Split only on the first occurrence
                explanation_part = parts[0].replace("EXPLANATION:", "", 1).strip() # Replace only the first occurrence
                precautions_part = parts[1].strip()

                explanation = explanation_part if explanation_part else explanation
                precautions = precautions_part if precautions_part else precautions

            except Exception as parse_error:
                 print(f"Error parsing Gemini summary response: {parse_error}")
                 print(f"Raw response that failed parsing:\n{response_text}")
                 # Fallback: Indicate parsing failure and provide raw response snippet
                 explanation = f"Summary parsing error. Raw response snippet: {response_text[:100]}..."
                 precautions = "Please consult a healthcare professional."


        else:
            print(f"Warning: Gemini response for summary did not contain expected markers (EXPLANATION:, PRECAUTIONS:):\n{response_text}")
            # Fallback: Use the raw response if markers are missing
            explanation = f"Summary format error. Raw response: {response_text[:100]}..." # Truncate raw response
            precautions = "Please consult a healthcare professional."


        return {"explanation": explanation, "precautions": precautions}

    except Exception as e:
        print(f"Error generating result summary with Gemini: {e}")
        # More specific error message if API call fails
        return {"explanation": f"Summary generation failed: {e}", "precautions": "Please consult a healthcare professional."}


# --- End Helper Functions ---


# --- Flask Routes ---
@app.route('/', methods=['GET'])
def index():
    """Renders the main index page."""
    # Pass the 'now' function to the template so it can display the current year
    return render_template('index.html', now=datetime.now)


@app.route('/about')
def about():
    """Renders the About page."""
    # Pass the 'now' function to the template for the footer
    return render_template('about.html', now=datetime.now)


@app.route('/predict', methods=['POST'])
def predict():
    """Handles image upload, prediction, and returns results as JSON."""

    if 'image' not in request.files:
        return jsonify({"error": "No image file uploaded"}), 400

    file = request.files['image']
    if file.filename == '':
         return jsonify({"error": "No selected file"}), 400

    if file and allowed_file(file.filename):
        filename = secure_filename(file.filename)

        # Construct the full path relative to the static folder for saving
        uploaded_image_path_relative_for_save = os.path.join(UPLOAD_FOLDER, filename)
        # Construct the absolute path for saving the file
        uploaded_image_path_absolute = os.path.join(app.root_path, uploaded_image_path_relative_for_save)

        # Ensure the uploads folder exists within the static directory
        uploads_folder_absolute = os.path.join(app.root_path, UPLOAD_FOLDER)
        if not os.path.exists(uploads_folder_absolute):
             os.makedirs(uploads_folder_absolute)


        try:
            # Save the uploaded file to the static uploads folder
            file.save(uploaded_image_path_absolute)
        except Exception as e:
             print(f"Error saving uploaded file: {e}")
             return jsonify({"error": f"Error saving uploaded file: {e}"}), 500


        # Perform prediction using the saved file's absolute path
        prediction_text, image_id, predicted_class_name = predict_image(uploaded_image_path_absolute)

        # Check if prediction was successful (image_id will be None on error)
        if image_id is None:
            # Clean up the potentially saved file if prediction failed
            if os.path.exists(uploaded_image_path_absolute):
                try:
                    os.remove(uploaded_image_path_absolute)
                    print(f"Cleaned up failed upload: {uploaded_image_path_absolute}")
                except Exception as e:
                    print(f"Error cleaning up file {uploaded_image_path_absolute}: {e}")
            return jsonify({"error": prediction_text}), 500


        # Find the corresponding modified image path (relative to static for url_for)
        matching_modified_image_path_relative = find_matching_modified_image(image_id)

        # Generate summary and precautions using Gemini
        summary_data = generate_result_summary(predicted_class_name)

        # Prepare response data as a dictionary to send back as JSON
        response_data = {
            "prediction": prediction_text, # This is the original comprehensive text
            "image_id": image_id,
            "predicted_class": predicted_class_name, # Include the class name in the response
            "uploaded_image_url": url_for('static', filename=f'uploads/{filename}'),
            "modified_image_url": None, # Initialize to None
            # Provide default error messages if summary_data is None or keys are missing
            "gemini_explanation": summary_data.get("explanation", "Could not generate explanation.") if summary_data else "Could not generate explanation.",
            "gemini_precautions": summary_data.get("precautions", "Could not generate precautions.") if summary_data else "Could not generate precautions."
        }

        if matching_modified_image_path_relative:
             # If modified image was found, get its URL using url_for.
            response_data["modified_image_url"] = url_for('static', filename=matching_modified_image_path_relative)
        else:
             # Add a flag if the modified image was not found
             response_data["modified_image_not_found"] = True


        # Return the response data as JSON
        return jsonify(response_data)

    # Return JSON error for disallowed file type
    return jsonify({"error": "File type not allowed"}), 400


@app.route('/chatbot', methods=['POST'])
def chatbot_response():
    """Handles chat messages and interacts with the Gemini API."""
    global chat_session # Declare chat_session as global to modify it

    # Check if the chatbot was successfully configured
    if gemini_model is None: # Check gemini_model instead of chat_session
        # Return JSON response indicating chatbot is unavailable
        return jsonify({"response": "Chatbot is not available due to API configuration issues."}), 500

    # Get the user message from the JSON request body sent by the frontend
    user_message = request.json.get('message')
    if not user_message:
        # Return JSON response if no message content is received
        return jsonify({"response": "No message received."}), 400

    try:
        # If chat_session is None, it's the first message, so start a new session with context
        if chat_session is None:
            # Provide context about the project and the chatbot's role
            # Enhanced context to enforce medical assistant role and domain restriction
            initial_context = """
            You are an AI medical assistant specialized in Diabetic Retinopathy (DR) and its associated risks (cardiac, glaucoma) based on retinal image analysis.
            Your purpose is to help users understand the analysis results (like DR severity: No_DR, Mild, Moderate, Severe, Proliferate_DR) and provide general, relevant information about these specific conditions.
            You are NOT a substitute for a doctor. You cannot provide medical diagnosis, treatment plans, or advice for any health issue beyond explaining the analysis results in the context of DR and its mentioned associated risks.
            If the user asks about any topic outside of Diabetic Retinopathy, cardiac risk, or glaucoma risk as they relate to retinal image analysis, politely inform them that you can only discuss topics relevant to the retinal analysis and associated risks and cannot provide general medical advice.
            Keep your responses informative, relevant, and always remind the user to consult a qualified healthcare professional for any medical concerns.
            """
            chat_session = gemini_model.start_chat(history=[
                {"role": "user", "parts": [initial_context]},
                {"role": "model", "parts": ["Understood. I am ready to assist with questions related to Diabetic Retinopathy analysis and its associated cardiac and glaucoma risks, as identified by this tool. Please remember I am not a substitute for a doctor."]}
            ])
            # Send the user's actual first message after the context is set
            response = chat_session.send_message(user_message)

        else:
            # If a session already exists, just send the user's message
            response = chat_session.send_message(user_message)

        # Get the text response from the bot
        bot_response = response.text

        # Return the bot's response as JSON
        return jsonify({"response": bot_response})

    except Exception as e:
        # Log the error on the server side
        print(f"Error interacting with Gemini API: {e}")
        # Return a user-friendly error message as JSON
        return jsonify({"response": f"Sorry, I couldn't process that. An error occurred."}), 500 # Avoid exposing internal error details to the user


# --- End Flask Routes ---


# --- App Startup ---
if __name__ == '__main__':
    # Ensure necessary static subdirectories exist when the app starts
    static_folder_absolute = os.path.join(app.root_path, 'static')
    uploads_folder_absolute = os.path.join(app.root_path, UPLOAD_FOLDER)
    modified_folder_absolute = os.path.join(app.root_path, MODIFIED_FOLDER)

    # Create static folder if it doesn't exist
    if not os.path.exists(static_folder_absolute):
         os.makedirs(static_folder_absolute)
         print(f"Created static folder at {static_folder_absolute}")

    # Create static/uploads folder if it doesn't exist
    if not os.path.exists(uploads_folder_absolute):
        os.makedirs(uploads_folder_absolute)
        print(f"Created uploads folder at {uploads_folder_absolute}")

    # Note: We don't automatically create the static/modified folder or its subdirectories
    # because this folder is intended to contain your pre-processed images.
    # We just check for its existence and print a message if it's not found.
    if not os.path.exists(modified_folder_absolute):
         print(f"Warning: Modified images folder not found at {modified_folder_absolute}. Modified images will not be displayed.")


    # Run the Flask development server
    # debug=True enables auto-reloading and debugger (turn off for production)
    app.run(debug=True)