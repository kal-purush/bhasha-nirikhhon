import streamlit as st
from keras.models import load_model
from PIL import Image
import numpy as np

from util import classify


# set title
st.title('Defects classification')

# set header
st.header('Please upload an image of the defect')

# upload file
file = st.file_uploader('', type=['jpeg', 'jpg', 'png'])

# load classifier
model = load_model('./model_c1.keras')

# load class names
with open('./labels.txt', 'r') as f:
    class_names = [line[:-1].split(' ')[1] for line in f.readlines()]

# display image
if file is not None:
    img = Image.open(file).convert('RGB')
    st.image(img, use_column_width=True)

    # classify image
    class_name, conf_score = classify(img, model, class_names)

    # write classification
    st.write("## {}".format(class_name))
    st.write("### score: {}%".format(conf_score))

