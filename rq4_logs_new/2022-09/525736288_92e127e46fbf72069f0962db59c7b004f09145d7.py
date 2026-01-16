import streamlit as st
import torch
import io
from PIL import Image
from pillow_heif import register_heif_opener

register_heif_opener()


@st.cache
def upload_model():
    YOLOmodel = torch.hub.load('ultralytics/yolov5', 'custom', path="YOLOmodel.pt")
    YOLOmodel.conf = 0.4

    return YOLOmodel


def detect(model):
    im_file = st.file_uploader("Upload picture")
    if im_file:

        im_bytes = im_file.read()
        im = Image.open(io.BytesIO(im_bytes))

        detected = model(im, size=640)

        st.image(detected.render(), use_column_width='auto')


model = upload_model()

st.markdown("# Upload your picture with a pet and the model will find it!")
st.text("")

detect(model)





