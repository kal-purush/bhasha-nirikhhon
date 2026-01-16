import streamlit as st
import numpy as np
import tensorflow as tf
from tensorflow.keras.models import load_model
from PIL import _imaging
from PIL import Image

# Lade das trainierte Modell
model = load_model('trained_model.h5')

# Funktion zur Reparatur des Bildes
def repair_image(input_image):
    # Vorverarbeitung des Bildes
    input_image = np.array(input_image)
    input_image = input_image / 255.0  # Normalisierung auf den Bereich [0, 1]

    # Vorhersage mit dem Modell
    predicted_image = model.predict(np.expand_dims(input_image, axis=0))

    # Nachverarbeitung des reparierten Bildes
    predicted_image = np.squeeze(predicted_image, axis=0)
    predicted_image = (predicted_image * 255).astype(np.uint8)  # Rückkehr zum ursprünglichen Bereich [0, 255]

    return predicted_image

# Streamlit-Anwendung
st.title('Bild Hochladen')

# Datei hochladen
uploaded_file = st.file_uploader("Bild hochladen", type=["png", "jpg", "jpeg"])

# Wenn eine Datei hochgeladen wurde
if uploaded_file is not None:
    # Zeige das hochgeladene Bild an
    st.image(uploaded_file, caption='Hochgeladenes Bild', use_column_width=True)

    # Repariere das Bild, wenn der Benutzer auf die Schaltfläche klickt
    if st.button('Reparieren'):
        # Öffne das hochgeladene Bild
        image = Image.open(uploaded_file)

        # Repariere das Bild
        repaired_image = repair_image(image)

        # Zeige das reparierte Bild an
        st.image(repaired_image, caption='Repariertes Bild', use_column_width=True)