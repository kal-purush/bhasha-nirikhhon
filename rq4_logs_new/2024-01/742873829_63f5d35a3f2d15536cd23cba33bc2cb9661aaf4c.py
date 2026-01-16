import streamlit as st
import numpy as np
import pandas as pd
import pickle

# importing model
model = pickle.load(open('model.pkl', 'rb'))
sc = pickle.load(open('standscaler.pkl', 'rb'))
ms = pickle.load(open('minmaxscaler.pkl', 'rb'))

# Streamlit app
st.title("Crop Recommendation System")

# User input form
N = st.text_input("Nitrogen")
P = st.text_input("Phosphorus")
K = st.text_input("Potassium")
temp = st.text_input("Temperature")
humidity = st.text_input("Humidity")
ph = st.text_input("Ph")
rainfall = st.text_input("Rainfall")

# Prediction button
if st.button("Predict"):
    # Forming feature list
    feature_list = [N, P, K, temp, humidity, ph, rainfall]
    single_pred = np.array(feature_list).reshape(1, -1)

    # Scaling features
    scaled_features = ms.transform(single_pred)
    final_features = sc.transform(scaled_features)

    # Making prediction
    prediction = model.predict(final_features)

    crop_dict = {
        1: "Rice", 2: "Maize", 3: "Jute", 4: "Cotton", 5: "Coconut", 6: "Papaya", 7: "Orange",
        8: "Apple", 9: "Muskmelon", 10: "Watermelon", 11: "Grapes", 12: "Mango", 13: "Banana",
        14: "Pomegranate", 15: "Lentil", 16: "Blackgram", 17: "Mungbean", 18: "Mothbeans",
        19: "Pigeonpeas", 20: "Kidneybeans", 21: "Chickpea", 22: "Coffee"
    }

    if prediction[0] in crop_dict:
        crop = crop_dict[prediction[0]]
        result = f"{crop} is the best crop to be cultivated right there"
    else:
        result = "Sorry, we could not determine the best crop to be cultivated with the provided data."

    st.success(result)