import pyttsx3
import sys
import sounddevice as sd
import soundfile as sf
import numpy as np
import time
import whisper
from PyQt5.QtWidgets import QApplication, QMainWindow, QPushButton, QHBoxLayout,        QLabel, QWidget, QVBoxLayout, QGridLayout, QTextEdit
from PyQt5.QtCore import Qt, QThread, pyqtSignal

class SpeechWorker(QThread):
    finished = pyqtSignal(str)
    def __init__(self):
        super().__init__()
        self.model = whisper.load_model("small")
        self.tts_engine = pyttsx3.init()
    def run(self):
        fs = 44100
        seconds = 5
        recording = sd.rec(int(seconds * fs), samplerate=fs, channels=1)
        sd.wait()
        audio_path0 = "output.wav"
        sf.write(audio_path0, recording,  fs)

        result = self.model.transcribe(audio_path0)
        print("helo", result)
        self.finished.emit(result["text"])

class MainWindow(QMainWindow):
    def __init__(self):
        super().__init__()

        self.setGeometry(100, 100, 800, 600)
        main_window = QMainWindow()
        self.setWindowTitle("My AI Assistant")

        central_widget = QWidget()
        layout = QVBoxLayout()

        self.setCentralWidget(central_widget)
        self.label = QLabel("")
        self.listen_button = QPushButton("Listen", self)
        self.textEdit = QTextEdit()

        layout.addWidget(self.listen_button)
        layout.addWidget(self.label)
        layout.addWidget(self.textEdit)
        central_widget.setLayout(layout)
        main_window.show()
        self.model = whisper.load_model("small")
        self.tts_engine = pyttsx3.init()
        self.listen_button.clicked.connect(self.listen)


    def listen(self):
        self.textEdit.append("Processing... Please wait")
        self.worker = SpeechWorker()
        self.worker.finished.connect(self.speak)
        self.worker.start()

    def speak(self, text):
        self.label.setText(text)
        self.textEdit.append(text)
        self.tts_engine.say(text)
        self.tts_engine.runAndWait()


def main():
    app = QApplication(sys.argv)
    window = MainWindow()
    window.show()
    sys.exit(app.exec_())


if __name__ == "__main__":
    main()