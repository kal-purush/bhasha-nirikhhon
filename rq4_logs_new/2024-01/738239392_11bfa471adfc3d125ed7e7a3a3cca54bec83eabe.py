import streamlit as st
import os
import numpy as np
from scipy.io.wavfile import read
# app.py
from speakerfeatures import extract_features
from excel import update_excel

import pickle

# Path to training data
source = "C:\\Users\\DELL\\Desktop\\Mini-Project\\Speaker-identification-using-GMMs\\development_set"
# Update the modelpath
modelpath = os.path.join("C:\\Users\\DELL\\Desktop\\Mini-Project\\Speaker-identification-using-GMMs", "dest_models")

# Debugging output to check files in the directory
print("Files in 'dest_models':", os.listdir(modelpath))

# Load the Gaussian gender Models
models = [pickle.load(open(os.path.join(modelpath, fname), 'rb')) for fname in os.listdir(modelpath) if fname.endswith('.gmm')]
speakers = [fname.split("\\")[-1].split(".gmm")[0] for fname in os.listdir(modelpath) if fname.endswith('.gmm')]


def detect_speaker(audio_path):
    sr, audio = read(audio_path)
    vector = extract_features(audio, sr)

    log_likelihood = np.zeros(len(models))

    for i in range(len(models)):
        gmm = models[i]
        scores = np.array(gmm.score(vector))
        log_likelihood[i] = scores.sum()

    winner = np.argmax(log_likelihood)
      # Update the Excel sheet with the detected speaker
    update_excel(audio_path, winner)
    return winner

def main():
    st.title("Speaker Identification Based Attendence Marking System")

    uploaded_file = st.file_uploader("Upload a .wav file", type=["wav"])
    
    if uploaded_file is not None:
        st.audio(uploaded_file, format='audio/wav')

        # Save the uploaded file
        file_path = "uploaded_file.wav"
        with open(file_path, "wb") as f:
            f.write(uploaded_file.getvalue())

        # Detect the speaker
        detected_speaker_index = detect_speaker(file_path)
        detected_speaker_name = speakers[detected_speaker_index]

        st.write(f"Detected Speaker: {detected_speaker_name}")
            # Footer
    st.markdown("---")
    st.write("The Detected Speaker Will be Marked Present ")


if __name__ == "__main__":
    main()