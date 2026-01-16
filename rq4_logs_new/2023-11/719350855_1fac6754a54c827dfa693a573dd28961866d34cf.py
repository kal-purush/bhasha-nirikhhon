import openai
import speech_recognition as sr
import pyttsx3
from pathlib import Path
import vlc
import os
import time

# Set your OpenAI API key
client = openai.OpenAI(
    api_key='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'
)

personality = "personality.txt"

with open(personality, "r") as file:
    mode = file.read()

messages=[
    {"role": "system", "content": f"{mode}"}    
  ]

r = sr.Recognizer()
mic = sr.Microphone(device_index=0)
r.dynamic_energy_threshold = False
r.energy_threshold = 400

e = True
while e:
    with mic as source:
        print("\nlistening...")
        r.adjust_for_ambient_noise(source, duration=0.5)
        audio = r.listen(source)
        try:
            user_input = r.recognize_google(audio)
        except:
            continue

    #user_input = input("question: ")
    print(f"\n{user_input}\n")

    if user_input == "exit program":
        e = False

    if e:
        messages.append({"role": "user", "content": user_input})

        completion = client.chat.completions.create(    
        model="gpt-3.5-turbo",
        messages=messages,
        temperature = 0.8
        )

        response_text = completion.choices[0].message.content
        speech_file_path = Path(__file__).parent / "speech.mp3"
        response = client.audio.speech.create(
            model="tts-1",
            voice="nova",
            input=response_text
        )
        messages.append({"role": "system", "content": response_text})
        
        print(f"\n{response_text}\n")
    
       
        response.stream_to_file(speech_file_path)
        
        p = vlc.MediaPlayer(speech_file_path)
        p.play()         
        time.sleep(1.5)
        duration = p.get_length() / 1000
        print(duration)
        time.sleep(duration)
       




