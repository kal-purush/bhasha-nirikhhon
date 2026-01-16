import streamlit as st
import joblib
from utils import predict_text

st.title("Sentiment Analysis")

# Input teks
input_text = st.text_area("Enter text here:")

if input_text:
    
    # Lakukan prediksi
    prediction = predict_text(input_text)

    # Tampilkan hasil
    st.write("Sentiment:")
    st.write(prediction)