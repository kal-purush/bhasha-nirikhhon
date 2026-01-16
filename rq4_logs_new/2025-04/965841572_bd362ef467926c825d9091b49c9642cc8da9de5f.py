import streamlit as st
import joblib
import os
import pandas as pd
from utils.logger import get_logger
from utils.preprocessing import preprocess_data_for_inference

logger = get_logger(__name__)

st.title("Real Estate Price Prediction App")
st.write("Enter the property details to predict the price.")

# Load the trained model
model_path = os.path.join("models", "model.pkl")
try:
    model = joblib.load(model_path)
    st.success("Model loaded successfully!")
    logger.info("Model loaded from %s", model_path)
except Exception as e:
    st.error("Failed to load the model. Please run the training script first.")
    logger.error("Error loading model: %s", e)
    st.stop()

# Input fields for the features you want the user to provide (4 features)
area = st.number_input("Area (sq ft)", min_value=100.0, value=1000.0)
bedrooms = st.number_input("Number of Bedrooms", min_value=1, value=3)
bathrooms = st.number_input("Number of Bathrooms", min_value=1, value=2)
age = st.number_input("Property Age (years)", min_value=0, value=10)

# Define the expected feature columns that your model was trained on (14 features total)
expected_columns = [
    "Area", "Bedrooms", "Bathrooms", "Age",
    "year_sold", "property_tax", "insurance",
    "lot_size", "basement", "popular",
    "recession", "property_age", "property_type_Bunglow", "property_type_Condo"
]

# Build a dictionary with the user inputs (for the 4 provided features)
input_data = {
    "Area": area,
    "Bedrooms": bedrooms,
    "Bathrooms": bathrooms,
    "Age": age
}

# For missing expected columns, add default values (you may change these defaults as needed)
for col in expected_columns:
    if col not in input_data:
        input_data[col] = 0  # default value, can be changed to something more suitable if needed

if st.button("Predict Price"):
    try:
        # Create the DataFrame ensuring columns are in the expected order
        df_input = pd.DataFrame([input_data], columns=expected_columns)
        
        # Preprocess the input data for inference
        X_input = preprocess_data_for_inference(df_input)
        
        # Predict using the trained model
        prediction = model.predict(X_input)
        
        st.write("Predicted Property Price: $", round(prediction[0], 2))
        logger.info("Prediction made with input: %s", input_data)
    except Exception as e:
        st.error("Error during prediction.")
        logger.error("Prediction error: %s", e)