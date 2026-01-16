import streamlit as st
import torch
from transformers import LayoutLMTokenizer, LayoutLMForSequenceClassification
from PIL import Image, ImageDraw
import pytesseract
import numpy as np
import requests
import zipfile
import io

# Load the trained model
model = LayoutLMForSequenceClassification.from_pretrained("microsoft/layoutlm-base-uncased")
tokenizer = LayoutLMTokenizer.from_pretrained("microsoft/layoutlm-base-uncased")

# Function to download and extract the image
def download_and_extract_image():
    url = "https://www.dropbox.com/s/kuw05qmc4uy474d/RVL_CDIP_one_example_per_class.zip?dl=1"
    r = requests.get(url)
    z = zipfile.ZipFile(io.BytesIO(r.content))
    z.extractall()
    return "/content/RVL_CDIP_one_example_per_class/resume/0000157402.tif"

# Function to perform OCR and inference
def perform_inference(image_path):
    # Perform OCR
    image = Image.open(image_path)
    ocr_df = pytesseract.image_to_data(image, output_type='data.frame')
    ocr_df = ocr_df.dropna().reset_index(drop=True)
    words = list(ocr_df.text)
    
    # Prepare input for the model
    encoding = tokenizer(' '.join(words), padding='max_length', truncation=True, return_tensors='pt')
    
    # Inference
    with torch.no_grad():
        logits = model(**encoding).logits
    
    # Get predicted label
    predicted_label_idx = torch.argmax(logits, dim=1).item()
    predicted_label = label_mapping[predicted_label_idx]
    
    return words, predicted_label

# Streamlit app
st.title("LayoutLM Inference App")

# Define label mappings
label_mapping = {
    0: 'memo',
    1: 'file_folder',
    2: 'invoice',
    3: 'form',
    4: 'scientific_publication',
    5: 'letter',
    6: 'questionnaire',
    7: 'presentation',
    8: 'news_article',
    9: 'budget',
    10: 'scientific_report',
    11: 'advertisement',
    12: 'email',
    13: 'specification',
    14: 'resume'
}

# Download and extract the image
image_path = download_and_extract_image()

# Perform inference
words, predicted_label = perform_inference(image_path)

# Display results
st.image(image_path, caption="Downloaded Image.", use_column_width=True)
st.write("")
st.write("Classifying...")

# Display results
st.write("OCR Output:")
st.write(words)
st.write("Predicted Label:", predicted_label)