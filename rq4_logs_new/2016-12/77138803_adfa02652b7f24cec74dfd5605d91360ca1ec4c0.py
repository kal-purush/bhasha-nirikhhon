#This script is built as a prototype during Mozilla HelloWeb Hackathon Kolkata 2016
#An Interactive Artificial Intelligence with a friendly personality to teach 5 year olds about HTML and WEB 
#Copyright Protected Under GPL3 License | Follow the License | Send Pull Requests
import re
import py
import requests
import pyaudio
import speech_recognition as sr
import os
import random
import socket                                                    
import webbrowser                                               #  facebook.com/ultimatepritam | github.com/ultimatepritam
import subprocess
import glob                                						# GRAPHICAL USER INTERfACE using 'Tkinter' immitating "DORA THE ExPLORER"
import time

##CONFIGURE THIS SECTION TO INDIAN LANGUAGES

# set property to voice engine
import pyttsx
engine = pyttsx.init('sapi5') #USE espeak IN LINUX
engine.setProperty('voice', 'HKEY_LOCAL_MACHINE\SOFTWARE\Microsoft\Speech\Voices\Tokens\TTS_MS_EN-US_ZIRA_11.0')
engine.setProperty('rate', 130)


# speak function 
def speak(text):
    engine.say(text)
    engine.runAndWait()

doss = os.getcwd()
i=0
n=0
flag=0
FACE = '''
        +=======================================+
        |.....JARVIS  ARTIFICIAL INTELLIGENCE...|
        +---------------------------------------+
        |#Author: ALienTrix                     |
        |#Date: 01/06/2016                      |
             ___    
            (   )                               
          .-.| |    .--.    ___ .-.      .---.  
         /   \ |   /    \  (   )   \    / .-, \ 
        |  .-. |  |  .-. ;  | ' .-. ;  (__) ; | 
        | |  | |  | |  | |  |  / (___)   .'`  | 
        | |  | |  | |  | |  | |         / .'| | 
        | |  | |  | |  | |  | |        | /  | | 
        | '  | |  | '  | |  | |        ; |  ; | 
        ' `-'  /  '  `-' /  | |        ' `-'  | 
         `.__,'    `.__.'  (___)       `.__.'_. 
        |                                       |
        +---------------------------------------+
        |.....JARVIS  ARTIFICIAL INTELLIGENCE...|
        +=======================================+
        |                                       |
        +=======================================+
        '''
print(FACE)
                                                   
while (i<1):
    r = sr.Recognizer()
    with sr.Microphone() as source:
        audio = r.adjust_for_ambient_noise(source)
        speak("Listening..")
        print("|-('')-|")						#START TALKING ONLY AFTER THIS PRINTS ON THE SCREEN
        audio = r.listen(source)
                                                   
    try:
        s = (r.recognize_google(audio))			#I used google gTTS as its the best Online recognizer, you can use CMU-SPHINX for OFFLINE
        message = (s.lower())
        print (message)

# PROFESSOR JARVIS ========================================================================================================== TRAINING MODULE
        if("teach me web") in message:
            rand = ['Oh My Goodness! You are only 5 years old! and you wanna know HTML?']
            speak(rand)
            speak("Okay, So HTML Stands for Hyper Text Markup Language! But lets not worry about this big word")
            speak("Now I'll do something for you, but lets first you cute little thing tell me whats your name ?")
            r = sr.Recognizer()
            with sr.Microphone() as source:
                audio = r.adjust_for_ambient_noise(source)
                #speak("Listening..")
                print(">>>")
                audio = r.listen(source)
                s = (r.recognize_google(audio))
                message = (s.lower())
                name=message
                print (name)
                speak('Oukay'+message+', So pretty name you have!')
                speak("Now Lets check this Cool thing I'm opening here...")
                Chrome = ("C:/Program Files (x86)/Google/Chrome/Application/chrome.exe %s")
                webbrowser.get(Chrome).open('KeepCalm.html')	#You might need to put the full path of the file here
                # B102 HARDWARE INTERRUPT NEEDED, CLOSE CHROME MANUALLY
                #time.sleep(10)
                #os.system('taskkill /im chrome.exe /f')

                speak("Do you see your name there? What? No?")
                print("10 sec up")
                os.system('atom KeepCalm.html')
                #subprocess.call(['notepad.exe','KeepCalm.html'])
                # To be done: Selenium Time Controlled Web Browser Monitor
                
                speak("Okay I am Opening something where you'll see some bla bla texts..")
                speak("You see it?")
                time.sleep(10)
                print("10 sec up")
                #HEADER
                speak("Look theres written Dora the explorer there, lets change it your name. You know you have a good cool name. Lets write it down here.")
                
                #os.system("notepad.exe keepcalm.html")
                #os.system('atom KeepCalm.html')
                time.sleep(10)
                print("10 sec up")
                speak("Now lets check the page with the Ugly naked guy again!")
                webbrowser.get(Chrome).open('KeepCalm.html')   #You might need to put the full path of the file here
                speak("can you see your name there now?")
                speak("You see it? Great!!")
                speak("You know its called a Header in html, grown ups write some big texts here!")

                #IMAGE IMPORT IN HTML
                speak("Oho! Everything is great but you don't have to be naked for that. Lets dress up, shall we?")
                speak("Now lets again check that stupid file with lots of bla bla texts in it")
                os.system('atom KeepCalm.html')
                speak("Do you see NAKED Guy written on somewhere? Can you see it? You found it? Yaaai!!")
                speak("Now lets change it to DRESSED Guy")
                speak("Now lets check the browser again")
                webbrowser.get(Chrome).open('KeepCalm.html') #You might need to put the full path of the file here
                speak("Yep! this looks better! You are a Smart kid!")
                speak("Now what you just did is how we change pictures in HTML")

                #STYLING IN HTML
                speak("Well since we are actually doing many cool stuffs! so that 'Keep calm and Do Nothing' phrase is little awkward. don't you think so?")
                speak("Now lets change it like you changed your name, Come on you can do it, I'm opening the same thing for you, its called an Editor, here you can write html codes")
                os.system('atom KeepCalm.html')
                speak("Now lets change the text to Keep calm and do Coding")
                time.sleep(10)
                print("10 sec up")
                speak("you did it, cool")
                speak("This portion of the code is called the Body you know! Body of the code!")
                speak("Now lets make the fonts look little bit bigger")
                speak("can you see there written font size?")
                speak("Change the value next to the font size 160 to 200 now")
                webbrowser.get(Chrome).open('KeepCalm.html')
                speak("You done it? Good Work!")
                speak("This thing just you did is called Styling. You know every cool kid likes fashion, our html too is cool. he likes styles. and it is called CSS")

				#FURTHER DEVELOPMENT GOES BELOW HERE INSIDE THE LOOP

    # exceptions
    except sr.UnknownValueError:
        print("$could not understand audio")
        speak("Pardon sir, can you please repeat?")
    except sr.RequestError as e:
        print("Could not request results$; {0}".format(e))