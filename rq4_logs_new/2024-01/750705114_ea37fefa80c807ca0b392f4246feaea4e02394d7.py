# pip install openai requests pydub pyaudio wave
import os
import requests  
from openai import OpenAI
from pydub import AudioSegment
from pydub.playback import play
import pyaudio
import wave
import sys

transcript = None
my_secret = "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"
client = OpenAI(api_key=my_secret)

# Set up Wit.ai token and API URL
WIT_API_URL = "https://api.wit.ai/message?v=20240129&"
WIT_TOKEN = "6WR6HLF4Q4WB5WYMQWTYMKJSYVAB6NRY"

# Set up requests session with Wit.ai token
wit_session = requests.Session()
wit_session.headers.update({
    'Authorization': f'Bearer {WIT_TOKEN}',
    'Content-Type': 'application/json',
})

def speechToText(file_path):
    audio_file = open(file_path, "rb")
    transcript = client.audio.transcriptions.create(
        model="whisper-1", 
        file=audio_file
    )
    print(transcript, "\n")  # recognized text

    # Extract the recognized text from the transcript object
    transcript_text = transcript.text

    return {
        'q': transcript_text
    }

def wit_response(a):
  pass


def handle_response(response):
  if response.status_code == 200:
      wit_response(response.json())
  else:
      print(f"Error {response.status_code}: {response.text}")

def traits(t):
  keys = t.keys()
  #print(keys)  # dict_keys(['wit$greetings'])
  for key in keys:
    if key == "wit$greetings":
      print(key)
      sound = AudioSegment.from_mp3("gajala.wav")
      print("Playing gajala sound ...")
      play(sound)

def wit_response(a):
  #print(a)
  #print(f"Entities --> {a['entities']}\n" if a['entities'] else "No Entities \n")
  #print(f"Intents --> {a['intents']}\n" if a['intents'] else "No Intents\n")
  #print(f"Traits --> {a['traits']}\n" if a['traits'] else "No Traits\n")
  if a['traits']:
      traits(a['traits'])




if __name__ == "__main__":
    if len(sys.argv) != 2:
        print("Usage: python main.py <file_path>")
        sys.exit(1)
    print(sys.argv)
    file_path = sys.argv[1]
    print(file_path)
    params = speechToText(file_path)
    response = wit_session.get(WIT_API_URL, params=params)
    handle_response(response)