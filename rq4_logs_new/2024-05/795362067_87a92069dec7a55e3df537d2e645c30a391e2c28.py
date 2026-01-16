import sys
import sounddevice as sd
import soundfile as sf
import whisper
from PyQt5.QtCore import Qt, QThread, pyqtSignal, QRunnable, QThreadPool
from PyQt5.QtGui import QIcon
from PyQt5.QtWidgets import QApplication, QMainWindow, QPushButton, QVBoxLayout, QTextEdit, QWidget
from playsound import playsound
from gtts import gTTS
from llm_integration import query_gpt
import pyaudio
from io import BytesIO
from pydub import AudioSegment
from pydub.playback import play
import numpy as np
from datetime import datetime
class SpeechWorker(QThread):
    finished = pyqtSignal(str)

    def __init__(self, model):
        super().__init__()
        self.model = model

    def run(self):
        fs = 44100
        seconds = 5
        audio_path = "output.wav"
        chunk = 1024
        # sample format
        FORMAT = pyaudio.paInt16
        # mono, change to 2 if you want stereo
        channels = 1
        f = 440
        samples = (np.sin(2 * np.pi * np.arange(fs * seconds) * f / fs)).astype(np.float32)

        p = pyaudio.PyAudio()
        stream = p.open(format=FORMAT,
                        channels=channels,
                        rate=fs,
                        input=True,
                        output=True,
                        frames_per_buffer=chunk)
        frames = []
        print("Recording...")
        for i in range(int(fs / chunk * seconds)):
            data = stream.read(chunk)
            # if you want to hear your voice while recording
            # stream.write(data)
            frames.append(data)
        print("Finished recording.")
        # stream.write(samples.tobytes())

        stream.stop_stream()
        stream.close()

        # audio_data = samples.tobytes()

        # Write audio data to a file using soundfile
        # sf.write('output2.wav', audio_data, fs, subtype='FLOAT')
        # recording = sd.rec(int(seconds * fs), samplerate=44100, channels=1)
        # sd.wait()
        # sf.write(audio_path, frames, fs)
        # sf.write(audio_path, frames, fs, subtype='FLOAT')
        # Convert frames to a numpy array with dtype int16
        audio_data = np.frombuffer(b''.join(frames), dtype=np.int16)

        # Save recorded audio to a file
        sf.write(audio_path, audio_data, fs)



        result = self.model.transcribe(audio_path)
        print("whisper output: ",result)

        if "language" in result:
            if result["language"] != "en":
                self.finished.emit("I am having trouble understanding your language")
            else:
                gpt_reply = query_gpt(result["text"])
                # gpt_reply = result["text"]
                self.finished.emit(gpt_reply)


class PlayWorker(QThread):
    finished = pyqtSignal(str)

    def __init__(self, text):
        super().__init__()
        self.text = text

    def run(self):
        print("start gtts", datetime.now().time())
        tts = gTTS(self.text, lang='en', tld='us')
        print("hehehh", tts)
        print("end gtts", datetime.now().time())

        mp3_fp = BytesIO()
        tts.write_to_fp(mp3_fp)
        print("end gtts write to byte", datetime.now().time())
        tts.save("audio_gtts.mp3")
        print("end gtts write to file", datetime.now().time())

        mp3_fp.seek(0)
        song = AudioSegment.from_file(mp3_fp, format="mp3")
        print("end gtts read from file", datetime.now().time())

        play(song)

        # audio_array = np.frombuffer(mp3_fp.getvalue(), dtype=np.int16)
        # play_obj = sa.play_buffer(audio_array, 2, 2, 44100)  # Adjust parameters as needed
        # play_obj.wait_done()


class MainWindow(QMainWindow):
    def __init__(self, model):
        super().__init__()
        self.model = model
        self.setGeometry(100, 100, 300, 400)
        self.setWindowTitle("My AI Assistant")
        self.central_widget = QWidget()
        self.setCentralWidget(self.central_widget)
        self.textEdit = QTextEdit()
        self.textEdit.setPlaceholderText("Speak into the microphone and your speech will appear here...")
        self.listen_button = QPushButton()
        self.listen_button.setIcon(QIcon('microphone.png'))
        self.listen_button.setFixedSize(48, 48)
        self.listen_button.clicked.connect(self.listen)
        self.layout = QVBoxLayout(self.central_widget)
        self.layout.addWidget(self.textEdit)
        self.layout.addWidget(self.listen_button, alignment=Qt.AlignRight)

    def listen(self):
        self.textEdit.append('')
        self.textEdit.append(f"<span style='color:grey;'>Listening</span>")
        self.worker = SpeechWorker(self.model)
        self.worker.finished.connect(self.speak)
        self.worker.start()

    def speak(self, text):
        # self.textEdit.append(f"<span style='color:grey;'>Processing...</span>")
        self.textEdit.append(text)
        self.worker1 = PlayWorker(text)
        self.worker1.start()




def main():
    app = QApplication(sys.argv)
    model = whisper.load_model("small")
    window = MainWindow(model)
    window.show()
    app.exec_()


if __name__ == "__main__":
    main()