import PyPDF2
import tempfile
import streamlit as st
from gtts import gTTS
from langdetect import detect
from mutagen.id3 import ID3, TIT2, TPE1
from mutagen.mp3 import MP3
from st_social_media_links import SocialMediaIcons

# Configuración: Caracteres a Braille y lenguajes soportados por gTTS
BRAILLE_MAP = {char: braille for char, braille in zip(
    "abcdefghijklmnopqrstuvwxyz0123456789 .,?!;-()[]{}:¿¡",
    "⠁⠃⠉⠙⠑⠋⠛⠓⠊⠚⠅⠇⠍⠝⠕⠏⠟⠗⠎⠞⠥⠧⠺⠭⠽⠵⠁⠃⠉⠙⠑⠋⠛⠓⠊⠚⠴⠂⠦⠔⠤⠰⠱⠸⠤⠴⠮⠬⠩⠎⠎"
)}
SUPPORTED_LANGUAGES = {"en": "en", "es": "es", "pt": "pt", "it": "it", "de": "de", "fr": "fr"}
DEFAULT_LANGUAGE = "en"

# Funciones auxiliares
def extract_text_from_pdf(pdf_file):
    reader = PyPDF2.PdfReader(pdf_file)
    return "".join(page.extract_text() for page in reader.pages[:50])  # Limitar a 50 páginas

def detect_language(text):
    return SUPPORTED_LANGUAGES.get(detect(text), DEFAULT_LANGUAGE)

def generate_audio(text, lang, slow, file_name):
    tts = gTTS(text, lang=lang, slow=slow)
    temp_audio = tempfile.NamedTemporaryFile(delete=False, suffix=".mp3", prefix=file_name)
    tts.save(temp_audio.name)

    # Añadir metadatos al archivo de audio
    audio = MP3(temp_audio.name, ID3=ID3)
    audio.add_tags()
    audio.tags.add(TIT2(encoding=3, text="Material Educativo"))
    audio.tags.add(TPE1(encoding=3, text="Generador Multilingüe"))
    audio.save()
    return temp_audio.name

def text_to_braille(text):
    return ''.join(BRAILLE_MAP.get(char.lower(), char) for char in text)

def split_text(text, max_length=10000):
    return [text[i:i + max_length] for i in range(0, len(text), max_length)]

# Aplicación Streamlit
st.image(
    "https://img.myloview.com.br/quadros/podcast-background-recording-studio-microphone-with-audio-waveform-broadcast-radio-or-podcasting-dark-banner-with-copy-space-700-256423810.jpg",
    use_container_width=True,
)
# Título con tamaño de fuente ajustable
st.markdown(
    '<h1 style="font-size: 28px;">GENERADOR DE TEXTO A AUDIO Y LENGUAJE BRAILLE </h1>',
    unsafe_allow_html=True
)
st.markdown(
    "###### Aplicacion que convierte texto a audio y a lenguaje Braille para fines educativos. ideal para personas con discapacidad visual. "
    "Soporta idioma Español, Inglés, Portugués, Italiano, Alemán, Francés."
)

input_method = st.radio("Selecciona el método de entrada:", ("Subir PDF", "Ingresar texto manualmente"))

if input_method == "Subir PDF":
    uploaded_file = st.file_uploader("Sube tu archivo PDF", type="pdf")
    if uploaded_file is not None:
        text = extract_text_from_pdf(uploaded_file)
        file_name = uploaded_file.name.split(".")[0]  # Extraer el nombre del archivo sin la extensión
    else:
        text = ""
        st.warning("Por favor, sube un archivo PDF válido para continuar.")
else:
    text = st.text_area("Ingresa tu texto aquí:", height=200)

if text.strip():
    st.text_area("Texto procesado:", text, height=200)
    lang = detect_language(text)
    st.write(f"Idioma detectado: {lang.upper()}")

    # Configuración de audio
    slow_audio = st.checkbox("Velocidad lenta", value=False)

    if st.button("Generar Audio y Braille"):
        # Generar audio y Braille
        braille_text = text_to_braille(text)
        text_parts = split_text(text)
        for idx, part in enumerate(text_parts):
            audio_file = generate_audio(part, lang, slow_audio, file_name)
            st.audio(audio_file, format="audio/mp3")
            with open(audio_file, "rb") as file:
                st.download_button(f"Descargar Audio Parte {idx + 1}", file, f"audio_parte_{idx + 1}.mp3")

        # Mostrar Braille
        st.subheader("Texto en Braille")
        st.text_area("Braille:", braille_text, height=200)
        with open(tempfile.NamedTemporaryFile(delete=False, suffix=".txt").name, "w") as f:
            f.write(braille_text)
        with open(f.name, "rb") as file:
            st.download_button("Descargar Braille", file, "texto_braille.txt")

else:
    st.warning("Sube un PDF o escribe texto para continuar.")

# Pie de página
st.markdown(
    """
    <div style="text-align: center;">
        <strong>Desarrollador:</strong> Edwin Quintero Alzate | <strong>Email:</strong> egqa1975@gmail.com
    </div>
    """, 
    unsafe_allow_html=True
)
SocialMediaIcons([
    "https://www.facebook.com/edwin.quinteroalzate",
    "https://www.linkedin.com/in/edwinquintero0329/",
    "https://github.com/Edwin1719",
]).render()