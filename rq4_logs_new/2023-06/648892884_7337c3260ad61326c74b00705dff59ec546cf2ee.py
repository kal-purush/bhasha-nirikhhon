import streamlit as st
import tensorflow as tf
from tensorflow import keras
from PIL import Image
import numpy as np
import pandas as pd
import pickle
from sklearn.preprocessing import StandardScaler
from keras.utils import img_to_array
from utils import classify
from utils import load_model
from utils import predict


def first_predictor(image):
    """
        This is the prediction function for those images that are uploaded manullay from the device in which the web
        app is running.
        :param image: Uploaded Image
        """
    # status = st.text("Loading model ...")
    model = load_model()
    # status.text("Loading model ... done!")
    # image = st.file_uploader("")
    if image is not None:
        st.image(image)
        # status.text("")
        button = st.button(label="Classify", key="classify_tab1")
        if button:
            prediction, confidence = predict(image, model)
            st.write("Prediction")
            st.subheader(prediction)
            st.write("Confidence")
            st.subheader(confidence)
            # col1, col2 = st.columns(2)
            # col1.metric("Predicted Class", prediction)
            # col2.metric("Confidence", confidence)


def second_predictor(image):
    """
        This is the prediction function for those images that are taken from the device camera in which the web app is running.
        :param image: The taken raw image
        """
    model = load_model()  # Loading our classifier

    if image is not None:
        st.image(image)

        button = st.button(label="Classify", key="classify_tab2")
        if button:
            prediction, confidence = predict(image, model)
            st.write("Prediction")
            st.subheader(prediction)
            st.write("Confidence")
            st.subheader(confidence)


def main():
    """
        This function is responsible for the startup and covers the UI of the system.
        Basically it creates three tabs having different functionalities
        """
    tab1, tab2, tab3 = st.tabs(
        ["Upload", "Use Camera", "Crop Recommendation"])  # Creating separate tabs for each Classifiers
    with tab1:
        tab1_image = st.file_uploader("Upload an Image")
        first_predictor(tab1_image)
    with tab2:
        # cam_button = st.button("Open Camera")
        # if cam_button:
        #         st.write("Hello world")
        tab2_image = st.camera_input("Take a picture")
        second_predictor(tab2_image)
    with tab3:
        nitrogen = st.text_input("Nitrogen Content")
        phosphorus = st.text_input("Phosphorus Content")
        potassium = st.text_input("Potassium Content")
        temperature = st.text_input("Temperature")
        humidity = st.text_input("Humidity")
        ph = st.text_input("ph value")
        rainfall = st.text_input("Rainfall")
        if st.button("Predict"):
            l = [nitrogen, phosphorus, potassium, temperature, humidity, ph, rainfall]
            arr = np.array(l, dtype=float)
            arr = np.expand_dims(arr, axis=0)
            print(arr)
            result = classify(arr)[0]
            st.subheader("You should consider growing :red[{}] in your field for this weather.".format(result.upper()))


with st.sidebar:
    st.header("About")
    st.markdown("An AI system which can not only identify disease in plants but also recommends suitable crops to "
                "grow in different climatic conditions.")
    st.sidebar.caption("Diagnose your plant using our CNN classifier")
    st.markdown("Made by [Sandeep Kashyap]()")
    st.header("Resources")
    st.markdown('''
        - [Dataset](https://www.kaggle.com/datasets/emmarex/plantdisease)
        - [Streamlit](https://docs.streamlit.io/)
        ''')

    st.sidebar.header('Deploy')
    st.sidebar.markdown(
        'You can quickly deploy Streamlit apps using [Streamlit Community Cloud](https://streamlit.io/cloud) '
        'in just a few clicks.')

if __name__ == '__main__':
    main()