import streamlit as st
import base64
from openai import OpenAI
from dotenv import load_dotenv
import os
import tempfile
import subprocess

# Load environment variables from a .env file
load_dotenv()

# Initialize OpenAI client
client = OpenAI(api_key="sk-o9pT3xIX1lod6-hdC98FY-ZCe9aFmsKpCET8F4PLI_T3BlbkFJyi-F8AieHSsZDWs25ujuIVRgEuJbFQLZv5tVfev3EA")

# Set page config
st.set_page_config(page_title="AI-Powered Video Translation App", layout="wide")

# Streamlit app
st.title("AI-Supported Startup Application")

# Background image styling
def add_bg_image():
    st.markdown(
        f"""
        <style>
        .stApp {{
            background-image: url("data:image/png;base64,{base64.b64encode(open("green2.jpg", "rb").read()).decode()}");
            background-size: cover;
            background-repeat: no-repeat;
            background-position: center;
        }}
        </style>
        """,
        unsafe_allow_html=True
    )

# Add the background image
add_bg_image()

# Validate uploaded file
uploaded_file = st.file_uploader("Upload an audio or video file", type=["mp3", "wav", "m4a", "mp4", "avi", "mov"])
if uploaded_file is not None:
    file_extension = os.path.splitext(uploaded_file.name)[1].lower()
    if file_extension not in ['.mp3', '.wav', '.m4a', '.mp4', '.avi', '.mov']:
        st.error("Unsupported file type.")
        st.stop()

# Language selection with a maximum limit (rate limiting)
languages = st.multiselect("Select target languages for translation (max 3)",
                           ["Turkish", "English", "German", "Spanish", "French", "Italian", "Russian"],
                           max_selections=3)

# Functions for saving files and processing audio/video
def save_uploaded_file(uploaded_file):
    if uploaded_file is not None:
        with tempfile.NamedTemporaryFile(delete=False, suffix=os.path.splitext(uploaded_file.name)[1]) as tmp_file:
            tmp_file.write(uploaded_file.getvalue())
            return tmp_file.name
    return None

def convert_video_to_audio(video_path):
    audio_path = os.path.splitext(video_path)[0] + ".wav"
    subprocess.call(['ffmpeg', '-i', video_path, '-acodec', 'pcm_s16le', '-ar', '44100', audio_path])
    return audio_path

def save_srt_file(content, filename):
    safe_filename = filename.replace("/", "_").replace("\\", "_")
    with open(safe_filename, 'w', encoding='utf-8') as f:
        f.write(content)
    return safe_filename

# Main process
if st.button("Transcribe and Translate") and uploaded_file is not None and languages:
    with st.spinner("Processing file..."):
        try:
            temp_file_path = save_uploaded_file(uploaded_file)

            # Check if it's a video file and convert to audio if necessary
            if file_extension in ['.mp4', '.avi', '.mov']:
                audio_file_path = convert_video_to_audio(temp_file_path)
                os.remove(temp_file_path)  # Remove the original video file for security
            else:
                audio_file_path = temp_file_path

            # Transcribe audio
            with open(audio_file_path, "rb") as audio_file:
                transcription = client.audio.transcriptions.create(
                    model="whisper-1",
                    file=audio_file,
                    response_format="srt"
                )
            st.success("Audio transcribed successfully!")

            # Save original transcription with a secure filename
            original_srt_path = save_srt_file(transcription, "original_transcription.srt")
            st.success(f"Original SRT file saved: {original_srt_path}")

            # Display original transcription
            st.subheader("Original Transcription (SRT)")
            st.text_area("Original SRT Content", transcription, height=200)
            st.download_button(
                label="Download Original SRT",
                data=transcription,
                file_name="original_transcription.srt",
                mime="text/plain"
            )

            # Translate transcription for each selected language
            for language in languages:
                response = client.chat.completions.create(
                    model="gpt-4",
                    messages=[
                        {"role": "system",
                         "content": "You are a very helpful and talented translator who can translate all languages and srt files."},
                        {"role": "user",
                         "content": f"Could you please translate the .srt text below to {language}? Do not add any comments of yours only the translation. "
                                    f"Please do not change the timestamps and structure of the file.\n<Transcription>{transcription}</Transcription>"}
                    ]
                )
                translated_srt = response.choices[0].message.content
                st.success(f"Translation to {language} completed!")

                # Save translated SRT securely
                translated_srt_path = save_srt_file(translated_srt, f"translated_subtitles_{language.lower()}.srt")
                st.success(f"Translated SRT file saved for {language}: {translated_srt_path}")

                # Display translated subtitles
                st.subheader(f"Translated Subtitles ({language})")
                st.text_area(f"SRT Content ({language})", translated_srt, height=200)

                # Download button for translated SRT
                st.download_button(
                    label=f"Download {language} Translated SRT",
                    data=translated_srt,
                    file_name=f"translated_subtitles_{language.lower()}.srt",
                    mime="text/plain"
                )

            # Clean up temporary audio file
            os.remove(audio_file_path)

        except Exception as e:
            st.error(f"An error occurred: {str(e)}")

# Instructions
st.sidebar.header("Instructions")
st.sidebar.markdown("""
1. Upload an audio or video file (mp3, wav, m4a, mp4, avi, or mov format).
2. Select one or more target languages for translation.
3. Click 'Transcribe and Translate' to process the file.
4. The original and translated SRT files will be saved automatically.
5. View the original transcription and translations in the app.
6. Download the original and translated SRT files if needed.
""")