import os
from datetime import datetime
import playsound
import speech_recognition as sr
from gtts import gTTS
import random
import webbrowser
import wikipedia

now = datetime.now()
current_time = now.strftime('%H:%M:%S')

def speak(text):
    text_to_speech = gTTS(text = text, lang = 'en', slow=False)
    r = random.randint(1,20)
    audio_file = f'audio- {str(r)} .mp3'
    text_to_speech.save(audio_file)
    playsound.playsound(audio_file)
    os.remove(audio_file)

def get_command(output):
    r = sr.Recognizer()
    with sr.Microphone() as source:
        voice_data = r.listen(source,timeout = 5)
        said = ''

        try:
            said = r.recognize_google(voice_data)
            print(said)
        except Exception as e:
            print("Sorry I couldn't get you")
    return said
if current_time < '24:00:00' and current_time < '12:00:00':
    speak('Good Morning Sir')
elif current_time < '16:00:00' and current_time > '12:00:00':
    speak('Good Afternoon Sir')
else:
    speak('Good Evening Sir')

if 'hello' or 'hi' in output:
    speak('Hello how are you')