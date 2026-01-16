from flask import Flask, request, redirect
from twilio.rest import TwilioRestClient
import speech_recognition as sr
from datetime import datetime
import twilio.twiml
import os, urllib

app = Flask(__name__)

VARS = {}
BASE = '/var/www/lifeprompt/'
# Read in environment variables
if os.path.exists(BASE + 'env-vars'):
    for line in open(BASE + 'env-vars'):
        var = line.strip().split('=')
        if len(var) == 2:
            VARS[var[0]] = var[1]

client = TwilioRestClient(VARS['ACCT_SID'], VARS['AUTH_TOK'])

def transcribe_recording(recording):
    """Use SpeechRecognition interface to Google SR API for transcription"""
    recog = sr.Recognizer()
    with sr.WavFile(recording) as source:
        audio = recog.record(source)
    try:
        #r.recognize_google(audio, Key="GOOGLE_SPEECH_RECOGNITION_API_KEY")
        return recog.recognize_google(audio)
    except sr.UnknownValueError:
        return "Google Speech Recognition could not understand audio"
    except sr.RequestError as err:
        return "Could not request results; {0}".format(err)

@app.route("/", methods=['GET', 'POST'])
def respond_call():
    """Actions to be taken when receiving a call."""
    from_number = request.values.get('From', None)
    resp = twilio.twiml.Response()

    if from_number == VARS['PHONE_NUM']:
        resp.say("Hey Stephen.")

        with resp.gather(numDigits=1, action="/handle-key", method="POST") as g:
            g.say("""To record an entry press 1.
                     Press 2 to play the previous entry.
                     Press any other key to start over.""")
    else:
        resp.say("Unauthorized caller")

    return str(resp)

@app.route("/outbound", methods=['GET', 'POST'])
def handle_outbound():
    """Actions to be taken for an outbound call."""
    resp = twilio.twiml.Response()
    resp.say("Hey Stephen. Gather your thoughts, it's time for an entry!")

    with resp.gather(numDigits=1, action="/handle-key", method="POST") as g:
        g.say("""To record an entry press 1.
                 Press any other key to start over.""")

    return str(resp)

@app.route("/handle-key", methods=['GET', 'POST'])
def handle_key():
    """Logic for interpreting key presses."""
    resp = twilio.twiml.Response()
    digit_pressed = request.values.get('Digits', None)
    if digit_pressed == "1":
        resp.say("Recording beginning. Press pound to end.")
        resp.record(maxLength="120", action="/handle-recording")
        return str(resp)

    elif digit_pressed == "2":
        resp.play(client.recordings.list()[0].uri)
        return str(resp)
    else:
        return redirect("/")

@app.route("/handle-recording", methods=['GET', 'POST'])
def handle_recording():
    """Retrieve and store WAV and TXT versions of the recorded entry."""
    resp = twilio.twiml.Response()
    recording_url = request.values.get("RecordingUrl", None)
    recording_file = urllib.urlretrieve(recording_url,
                                        "{0}entries/{1}.wav"
                                        .format(BASE, datetime.utcnow()))

    transcription = transcribe_recording(recording_file[0])
    print transcription
    with open(recording_file[0][0:-3] + 'txt', 'w') as ts_file:
        ts_file.write(transcription)

    resp.say("Entry stored. Goodbye.")
    return str(resp)

if __name__ == "__main__":
    app.run()