import streamlit as st
from PIL import Image
import matplotlib.pyplot as plt
import base64
import io
from langchain_core.messages import HumanMessage


# Set page configuration
st.set_page_config(page_title="Helper", page_icon="😵", layout="centered")


# Title of the app
st.markdown('''
    <h1 style="
        font-family: 'Arial', sans-serif;
        text-align: center;
        color: #4B0082;
        font-size: 3em;
        margin: 20px 0;
    ">
    Image insights generator    
    </h1>
    ''',
    unsafe_allow_html=True
)


# user input image
if 'imageinput' not in st.session_state:
    image = st.file_uploader(
        label='**Upload your image here**',
        key='imageinput'
    )
else:
    image = st.session_state['imageinput']
    st.file_uploader(
        label='**Image already uploaded**',
        disabled=True
    )


# buttons for app features
btn1, btn2 = False, False
if image:
    st.session_state['imageinput'] = image
    col1, col2, col3 = st.columns([0.4,0.4,0.2], gap='medium', vertical_alignment='center')
    with col1:
        btn1 = st.button(label='**Scene understanding**', type='primary', use_container_width=True)
    with col2:
        btn2 = st.button(label='**Obstacle detection**', type='primary', use_container_width=True)
    with col3:
        if st.button(label='**Clear input**', type='secondary', use_container_width=True):
            st.session_state.clear()
            st.rerun()


# loading api key
file = open("keys/geminiapi.txt")
key = file.read()


# MODELLING
from langchain_google_genai import ChatGoogleGenerativeAI
# Set the OpenAI Key and initialize a ChatModel
chat_model = ChatGoogleGenerativeAI(google_api_key=key, model="gemini-1.5-flash")


if btn1:
    btn1 = False
    img = Image.open(image)

    img_bytes1 = io.BytesIO()
    img.save(img_bytes1, format='JPEG')
    base64_image = base64.b64encode(img_bytes1.getvalue()).decode('utf-8')

    message = HumanMessage(
        content=[
            {"type": "text", "text": "Please describe the content of this image."},
            {
                "type": "image_url",
                "image_url": {"url": f"data:image/jpeg;base64,{base64_image}"},
            },
        ]
    )

    response = chat_model.invoke([message])

    st.write(response.content)
    with st.columns([0.1,0.8,0.1])[1]:
        st.image(image)


elif btn2:
    btn2 = False
    img = Image.open(image)
    
    buffered1 = io.BytesIO()
    img.save(buffered1, format="JPEG")
    img_data = base64.b64encode(buffered1.getvalue()).decode("utf-8")

    message = f"Identify and describe any obstacles in this image:\n\n![Image](data:image/jpeg;base64,{img_data})"

    response = chat_model.invoke([{"role": "user", "type": "text", "content": message}])

    from langchain_core.output_parsers import StrOutputParser
    parser = StrOutputParser()
    parsed_output = parser.parse(response.content)

    st.write(parsed_output)
    with st.columns([0.1,0.8,0.1])[1]:
        st.image(image)