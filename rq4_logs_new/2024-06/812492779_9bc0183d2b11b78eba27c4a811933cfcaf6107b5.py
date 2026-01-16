import streamlit as st
import pickle
from sklearn.feature_extraction.text import TfidfVectorizer
from sklearn.svm import SVC
import pandas as pd
from io import BytesIO
from PIL import Image
import base64

# Fungsi untuk memuat gambar sebagai base64
def load_image_as_base64(image_path):
    image = Image.open(image_path)
    buffered = BytesIO()
    image.save(buffered, format="PNG")
    return base64.b64encode(buffered.getvalue()).decode()

# Fungsi untuk pemrosesan teks
def lowercase(text):
    return text.lower()

def removepunc(text):
    return ''.join([char for char in text if char.isalnum() or char.isspace()])

def remove_sw(text):
    stopwords = set(['the', 'and', 'is', 'in', 'to', 'with'])
    return ' '.join([word for word in text.split() if word not in stopwords])

def stem_text(text):
    # Implementasi stemming yang sesuai
    return text  # Ganti dengan logika stemming yang sesuai

# Set the background colors and text color
st.markdown(
    """
    <style>
    body {
        background-color: #d3d3d3; /* Light gray background */
        margin: 0; /* Remove default margin for body */
        padding: 0; /* Remove default padding for body */
    }
    .st-bw {
        background-color: #eeeeee; /* Light gray background for widgets */
    }
    .st-cq {
        background-color: #cccccc; /* Gray background for chat input */
        border-radius: 10px; /* Add rounded corners */
        padding: 8px 12px; /* Add padding for input text */
        color: black; /* Set text color */
    }
    .st-cx {
        background-color: #d3d3d3; /* Light gray background for chat messages */
    }
    .sidebar .block-container {
        background-color: #d3d3d3; /* Light gray background for sidebar */
        border-radius: 10px; /* Add rounded corners */
        padding: 10px; /* Add some padding for spacing */
    }
    .top-right-image-container {
        position: fixed;
        top: 30px;
        right: 0;
        padding: 20px;
        background-color: #d3d3d3; /* Light gray background for image container */
        border-radius: 0 0 0 10px; /* Add rounded corners to bottom left */
    }
    .custom-font {
        color: #888888; /* Set font color to gray */
    }
    </style>
    """,
    unsafe_allow_html=True
)

# Load the logo image
logo_base64 = load_image_as_base64("BookGenie.png")

st.markdown(
    f"""
    <div style='display: flex; align-items: center; gap: 15px;' class='custom-font'>
        <img src='data:image/png;base64,{logo_base64}' width='50'>
        <h1 style='margin: 0;'>BookGenie</h1>
    </div>
    """,
    unsafe_allow_html=True
)

# Kolom input teks untuk deskripsi buku
book_description = st.text_area("Masukkan deskripsi buku:")

if st.button("Prediksi"):
    if not book_description:
        st.warning("Mohon isi deskripsi buku terlebih dahulu.")
    else:
        st.info("Sedang melakukan prediksi...")

        # Load model SVM dan vectorizer
        with open("C:\\Skilvul Grup 22 Education\\bookgenie-genreprediction\\svm_model.pkl", 'rb') as file:
            loaded_model = pickle.load(file)
        
        with open("C:\\Skilvul Grup 22 Education\\bookgenie-genreprediction\\tfidf_vectorizer.pkl", 'rb') as file:
            tfidf = pickle.load(file)
        
        # Preprocessing deskripsi buku
        book_description_processed = [stem_text(remove_sw(removepunc(lowercase(book_description))))]

        # Membaca data X_train
        X_train = pd.read_csv("C:\\Skilvul Grup 22 Education\\bookgenie-genreprediction\\X_train_tfidf.csv")  # Ubah sesuai dengan lokasi yang benar
        
        # Menerapkan pemrosesan teks pada data X_train
        X_train['Combined_Text'] = X_train['Combined_Text'].apply(lowercase)
        X_train['Combined_Text'] = X_train['Combined_Text'].apply(removepunc)
        X_train['Combined_Text'] = X_train['Combined_Text'].apply(remove_sw)
        X_train['Combined_Text'] = X_train['Combined_Text'].apply(stem_text)
        
        # Membuat dan melatih tfidf vectorizer dari data X_train
        tfidf = TfidfVectorizer(max_features=40530)  # Atur max_features sesuai kebutuhan
        X_train_tfidf = tfidf.fit_transform(X_train['Combined_Text']).toarray()

        # Transformasi deskripsi buku menggunakan TfidfVectorizer yang dimuat
        book_description_tfidf = tfidf.transform(book_description_processed).toarray()

        # Prediksi genre buku
        predictions = loaded_model.predict(book_description_tfidf)

        # Map hasil prediksi ke genre yang sesuai
        genre_mapping = {
            0: "adventure",
            1: "crime",
            2: "fantasy",
            3: "learning",
            4: "romance",
            5: "thriller"
        }
        
        predicted_genre = genre_mapping[predictions[0]]

        # Tampilkan hasil prediksi
        st.write("Hasil Prediksi:")
        st.title(predicted_genre)
        st.success("Prediksi selesai!")