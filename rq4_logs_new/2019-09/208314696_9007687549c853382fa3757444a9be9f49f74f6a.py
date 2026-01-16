# speak and find

import speech_recognition as sr
import webbrowser as wb

r1 = sr.Recognizer()
r2 = sr.Recognizer()
r3 = sr.Recognizer()

with sr.Microphone() as source:
    print("search in udemy website-> say website")
    print("search youtube video -> say video ")
    print('speak now')
    audio = r3.listen(source)
if 'website' in r2.recognize_google(audio):
    r2 = sr.Recognizer()
    url = 'https://www.udemy.com/'
    with sr.Microphone() as source:
        print('Which course you want to search in UDEMY???')
        audio = r2.listen(source)

        try:
            get = r2.recognize_google(audio)
            print(get)
            wb.get().open_new(url+get)
        except sr.UnknownValueError:
            print('error')
        except sr.RequestError as e:
            print('failed'.format(e))


if 'video' in r1.recognize_google(audio):
    r1 = sr.Recognizer()
    url = 'https://www.youtube.com/results?search_query='
    with sr.Microphone() as source:
        print('What you want to search in youtube ???')
        audio = r1.listen(source)

        try:
            get = r1.recognize_google(audio)
            print(get)
            wb.get().open_new(url+get)
        except sr.UnknownValueError:
            print('error')
        except sr.RequestError as e:
            print('failed'.format(e))

