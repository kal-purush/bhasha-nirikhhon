import sys

import pyttsx3
import sounddevice as sd
import soundfile as sf
import whisper
from PyQt5.QtCore import Qt, QThread, pyqtSignal
from PyQt5.QtGui import QIcon
from PyQt5.QtWidgets import QApplication, QMainWindow, QPushButton, QVBoxLayout, QTextEdit, QWidget

from llm_integration import query_gpt
from whisper import transcribe
from custom_func import cli
from whisper.model import Whisper

Base_URL = "/Users/mac1/Documents/Django/rasaAI/myAiAssistant/"
def run():
    audio_path = f"{Base_URL}output.wav"
    model = whisper.load_model("small")

    # result = model.transcribe(audio_path)
    result = cli(audio=audio_path)
    text= result["text"]
    print(result)


run()