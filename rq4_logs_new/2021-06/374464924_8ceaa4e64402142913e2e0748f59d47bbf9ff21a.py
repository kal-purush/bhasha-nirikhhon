import pyttsx3
import datetime
import speech_recognition as sr
# import wikipedia
import webbrowser
import os
import random
import smtplib
# it helps in sending email by using your gmail account


# for taking voices, we are going to use microsoft sapi5 (speech- api)
engine = pyttsx3.init('sapi5')
voices=engine.getProperty('voices')
# you can select the type of voice, either it can be a male on or female one. print(voices) will give two voice objects.
#print(voices) 
# to set the voice property of program
engine.setProperty('voice', voices[1].id)
# print(voices[0].id) ---voice id of david
# print((voices[1].id)) ---- voice id of zira




def speak(audio):
    engine.say(audio)
    engine.runAndWait()


def WelcomeWish():
    hour = int(datetime.datetime.now().hour)
    if hour >=0 and hour<12:
        speak("Good Morning Balram Sir!")
    elif hour>=12 and hour<18:
        speak("Good afternoon Balram Sir!")
    else:
        speak("Good Eve Balram Sir!")

    speak("I am your virtual assistant ashley, please tell me what can i do for you")



# import speechRecognition module for taking voice commands
def takeVoiceCommand():
    """it takes voice input microphone command from the user and return output as a string"""
    r = sr.Recognizer()
    with sr.Microphone() as source:
        print("Listening.......")
        r.pause_threshold = 1 # seconds of non-speaking audio before a phrase is considered complete
        audio = r.listen(source)
    
    try:
        print("Recognizing command")
        query = r.recognize_google(audio, language= 'en-in')
        print(f"user said: {query}\n")

    except Exception as e:
        print("say that again please")
        return "None"
    
    return query


def sendEmail(to, content):
    server = smtplib.SMTP('smtp.gmail.com', 587)
    server.ehlo()
    server.starttls()
    # email id by which you will send the email
    server.login("sender_email", 'password')
    server.sendmail('sender_email', to, content)
    server.close()



if __name__== '__main__':
    WelcomeWish()
while True:   
    query = takeVoiceCommand().lower()

    if 'wikipedia' in query:
        speak("searching on wikipedia")
        query = query.replace('wikipedia', '')
        results = wikipedia.summary(query, sentences = 3)
        speak("according to wikipedia")
        speak(results)

    elif "open youtube" in query:
        webbrowser.open('youtube.com')

    elif "open google" in query:
        webbrowser.open('google.com')

    # elif "on google" in query:
    #     speak("say what you want to search on google")
    #     search_command = takeVoiceCommand()
    #     #print(search_command)
    #     chrome_path = r"C:\\Program Files\\Google\\Chrome\\Application\\chrome.exe"
    #     webbrowser.register("chrome", None, webbrowser.BackgroundBrowser(chrome_path))
    #     webbrowser.get("chrome").open_new_tab(search_command+".com")

    elif "open twitter" in query:
        webbrowser.open('twitter.com')
    
    elif "open instagram" in query or 'open insta' in query:
        webbrowser.open('instagram.com')

    elif "open facebook" in query or 'open fb' in query:
        webbrowser.open('youtube.com')

    elif "open stackoverflow" in query:
        webbrowser.open('stackoverflow.com')
    
    elif "play music" in query:
        music_directory = "F:\\music downloaded"
        song_list = os.listdir(music_directory)
        print(song_list)
        random_num = random.randint(0, len(song_list)-1)
        os.startfile(os.path.join(music_directory, song_list[random_num]))

    elif "the time" in query:
        current_time = datetime.datetime.now().strftime("%H:%M")
        speak(f"the time is {current_time}")

    elif "vscode app" in query:
        vscode_path = "C:\\Users\\balra\\AppData\\Roaming\\Microsoft\\Windows\\Start Menu\\Programs\\Visual Studio Code"
        os.startfile(vscode_path)

    elif "email to" in query:
        try:
            speak("what should i write in the mail sir?")
            content = takeVoiceCommand()
            # email id of the receiver
            to= "receiver_email"
            sendEmail(to, content)

            speak("email has been sent!")
        except Exception as e:
            
            speak("email was not sent, kindly check the email id and I M A P settings so that email can be sent")
    
    elif "quit" in query or "bye" in query:
        exit()
            

    

