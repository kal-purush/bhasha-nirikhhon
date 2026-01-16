import os
import urllib.request
import ssl
import whisper

# SSL doğrulamasını devre dışı bırak
ssl._create_default_https_context = ssl._create_unverified_context

# Whisper modelini yükleyin
model = whisper.load_model("base")

# Ses dosyasını yazıya dökün
result = model.transcribe("/Users/cantaha/Documents/Kodlama/Python/Python_Mini_Projeler/public_audio.wav")

# Yazıya dökülen metni alın
transcription = result["text"]
print(transcription)