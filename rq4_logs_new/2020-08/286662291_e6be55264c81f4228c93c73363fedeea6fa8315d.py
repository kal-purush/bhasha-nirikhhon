'''import wikipedia
from nltk.corpus import stopwords
import nltk
from nltk import ne_chunk

a = wikipedia.summary('php',sentences=2)
tokens = nltk.word_tokenize(a)
c = stopwords.words('english')

de = [i for i in tokens if i not in c]
new = nltk.pos_tag(de)
chunk = ne_chunk(new)

chunk.draw()'''


#TEXT TO SPEECH
import pyttsx3
engine = pyttsx3.init()
#rate = engine.getProperty('rate')
#engine.setProperty('rate', rate-60)
voices = engine.getProperty('voices')
for voice in voices:
   engine.setProperty('voice', voice.id)
   engine.say('The quick brown fox jumped over the lazy dog.')
engine.runAndWait()


#SPEECH TO TEXT
import speech_recognition as sr
r = sr.Recognizer()
with sr.Microphone() as source:
    print('listening...')
    audio = r.listen(source)
    t = r.recognize_google(audio)
    print(t)


#ques2 of assignment
'''import datetime
import pyttsx3
import speech_recognition as sr
import wikipedia
import webbrowser
import os
#using pyttsx3
engine = pyttsx3.init('sapi5')
voices = engine.getProperty('voices')
engine.setProperty('voices',voices[0].id)


def speak(audio):   #speak function
    engine.say(audio)
    engine.runAndWait()


def wishMe():    #for wishing morning,evening or night
    hour=int(datetime.datetime.now().hour)
    if hour>=6 and hour<12:
        speak("Good Morning")
    elif hour>=0 and hour<16:
        speak("Good Afternoon")
    elif hour>=16 and hour<19:
        speak("Good Evening")
    else:
        speak("good night")
    speak(" I am Assistant Maam. Please tell me how may i help you")


def takeCommand():
    # it takes microphone input from user and returns string output
    r = sr.Recognizer()
    with sr.Microphone() as source:
        print("Listening...")
        sr.pause_threshold = 1
        audio = r.listen(source)

    try:
        print("Recognizing...")
        query = r.recognize_google(audio)
        print(f"user said:{query}\n")   
    except :
        print("Say that again please...")
        return"None"
    return query


if _name_ == "_main_":
    wishMe()
    takeCommand()
    #while True:
    if 1:
        query=takeCommand().lower() #query converted into lower case so can be matched easily

        #logic for executing task
        if 'wikipedia' in query: #for wikipedia search
           query = query.replace("wikipedia","")
           results = wikipedia.summary(query, sentences=2)
           print(results)
           speak("according to Wikipiedia")
           speak(results)
        elif 'open youtube ' in query:
            webbrowser.open("youtube.com") #for youtube search
        elif 'open google ' in query:
            webbrowser.open("google.com")
        elif 'the time' in query:     #telling the time
            strTime = datetime.datetime.now().strftime("%H:%M:%S")
            speak(f"Maam, the time is {strTime}")
        else:
            speak("Sorry Maam, i can not find it")'''