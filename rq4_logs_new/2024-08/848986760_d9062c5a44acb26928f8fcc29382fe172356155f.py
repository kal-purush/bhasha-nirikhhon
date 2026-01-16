import streamlit as st
import numpy as np
import tensorflow as tf

# Load the pre-trained model
model = tf.keras.models.load_model('toxic_model.h5')

# Streamlit App Title and Description
st.title("Molecular Toxicity Prediction App")

st.write("""
### Enter the values for the following molecular descriptors:
""")

# Create input fields for the features
nHBDon_Lipinski = st.number_input('Enter value for nHBDon_Lipinski', min_value=0.0, max_value=100.0, value=0.0, step=0.1)
nHBDon = st.number_input('Enter value for nHBDon', min_value=0.0, max_value=100.0, value=0.0, step=0.1)
AVP_2 = st.number_input('Enter value for AVP-2', min_value=0.0, max_value=100.0, value=0.0, step=0.1)
AVP_0 = st.number_input('Enter value for AVP-0', min_value=0.0, max_value=100.0, value=0.0, step=0.1)
AVP_1 = st.number_input('Enter value for AVP-1', min_value=0.0, max_value=100.0, value=0.0, step=0.1)
ASP_0 = st.number_input('Enter value for ASP-0', min_value=0.0, max_value=100.0, value=0.0, step=0.1)
ETA_dEpsilon_D = st.number_input('Enter value for ETA_dEpsilon_D', min_value=0.0, max_value=100.0, value=0.0, step=0.1)
nHBd = st.number_input('Enter value for nHBd', min_value=0.0, max_value=100.0, value=0.0, step=0.1)
ASP_1 = st.number_input('Enter value for ASP-1', min_value=0.0, max_value=100.0, value=0.0, step=0.1)
MATS1v = st.number_input('Enter value for MATS1v', min_value=0.0, max_value=100.0, value=0.0, step=0.1)

# Button to trigger prediction
if st.button("Predict Toxicity"):
    # Collect input data into a list
    input_data = [
        nHBDon_Lipinski, nHBDon, AVP_2, AVP_0, AVP_1,
        ASP_0, ETA_dEpsilon_D, nHBd, ASP_1, MATS1v
    ]
    
    # Convert input data to NumPy array and reshape it for the model
    input_data = np.array(input_data).reshape(1, -1)
    
    # Make prediction using the loaded model
    prediction = model.predict(input_data)
    
    # Extract the prediction result
    prediction_value = prediction[0][0]
    
    # Determine the result based on the prediction threshold
    result = 'Toxic' if prediction_value > 0.5 else 'Non-Toxic'
    
    # Display the prediction result
    st.write(f"The predicted output is: **{result}**")