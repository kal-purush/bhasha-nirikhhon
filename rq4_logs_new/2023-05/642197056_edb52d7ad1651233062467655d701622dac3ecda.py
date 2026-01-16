import streamlit as st
import pandas as pd

st.subheader("Loading CSV files")
file = st.file_uploader("Upload File Here", type=['csv', 'xlsx'])
if file:
    data = pd.read_csv(file)
    st.table(data.head())

st.subheader("Image Files")
img = st.file_uploader('Upload Your Image Here..', type=['png', 'jpg'])
if img:
    st.image(img)

video = st.file_uploader('Upload Video Here', type=['mkv', 'mp4'])
if video:
    st.video(video)

audio = st.file_uploader('Upload Audio file', type=['mp3', 'wav'])
if audio:
    st.audio(audio)