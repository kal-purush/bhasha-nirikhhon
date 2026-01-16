import pyttsx3
import sys
import sounddevice as sd
import numpy as np
import whisper
from PyQt5.QtWidgets import QApplication, QMainWindow, QPushButton


class MainWindow(QMainWindow):
    def __init__(self):
        super().__init__()
        self.listen_button = QPushButton("Listen", self)
        self.listen_button.clicked.connect(self.listen)
        self.listen_button.resize(200, 50)
        self.listen_button.move(300, 275)
        self.setWindowTitle("My AI Assistant")
        self.setGeometry(100, 100, 800, 600)

        #initialize whisper model
        self.model = whisper.load_model("small")

        #initialize text-to-speech engine
        self.tts_engine = pyttsx3.init()

    def listen(self):
        # Define the callback function to capture audio data
        def callback(indata, frames, time, status):
            if status:
                print(status)
            audio_queue.extend(indata.copy())

        # Initialize the audio queue and start recording
        audio_queue = []
        with sd.InputStream(callback=callback):
            sd.sleep(5000)  # Capture audio for 5 seconds

        # Convert the recorded audio data to a format suitable for Whisper
        audio_data = np.concatenate(audio_queue, axis=0).flatten()
        audio_path = "temp_audio.wav"
        sd.write(audio_path, audio_data, 44100)

        # Use Whisper to transcribe the audio
        result = self.model.transcribe(audio_path)
        self.speak(result["text"])

    def speak(self, text):
        self.tts_engine.say(text)
        self.tts_engine.runAndWait()


def main():
    app = QApplication(sys.argv)
    window = MainWindow()
    window.show()
    sys.exit(app.exec_())


if __name__ == "__main__":
    main()