import streamlit as st
from config.config import parse_args
from scripts.test import get_prediction


args = parse_args()

st.set_page_config(
     page_title="Flower Classifier App",
     page_icon="🌼",
     layout="centered",
     initial_sidebar_state="expanded")

m = st.markdown("""
<style>
div.stButton > button:first-child {
    background-color: rgb(204, 49, 49);
}
</style>""", unsafe_allow_html=True)


st.title('Flower classifier')
st.image('data/application/front_page.jpg')
st.subheader('Load flowers images')

uploaded_file = st.file_uploader("",
                                 accept_multiple_files=True,
                                 help='Drop a picture of flower in drag zone')

generate_pred = st.button('Predict')

if generate_pred:
    if len(uploaded_file) != 0:
        for image in uploaded_file:
            class_name = get_prediction(args, image, args.trained_model)
            st.title(f'Flower is - {class_name.upper()}!')
            st.image(image)
    else:
        st.title(f'There is nothing to predict....')