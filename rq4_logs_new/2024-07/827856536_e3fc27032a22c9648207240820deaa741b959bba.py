import random
from gtts import gTTS
import os
import pygame

import speech_recognition as sr
import pyttsx3
r = sr.Recognizer()


def SpeakText(command):
    # Initialize the engine
    engine = pyttsx3.init()
    engine.say(command)
    engine.runAndWait()

def responses(x):
    language = 'en'
    # array containing the responses assistant should give
    arr = ["hello boss", "hello sir", "how may i help you boss", "good evening boss"]
    # the array which contains the possible prompts given by the user
    guess=["hello there", "hey there", "how may i help you today","hii", "hey","hello",]
    for prompt in guess:
        if x == prompt:
            resp = random.randint(0, len(arr)-1)
            # myobj=gTTS(text=arr[resp], lang=language, slow=False)
            myobj = arr[resp]
            SpeakText(myobj)
            # myobj.save('first.mp3')
            # pygame.mixer.init()
            # pygame.mixer.music.load("first.mp3")
            # pygame.mixer.music.play()

            # wait for the audio to finish
            # while pygame.mixer.music.get_busy():
            #     pygame.time.Clock().tick(10)
            # # remove the file for new audio generation
            # pygame.mixer.music.stop()  # Stop the music to release the file
            # pygame.mixer.quit()  # Quit mixer to release resources
            # os.remove("first.mp3")
            break


    else: SpeakText("sorry i have not quite learnt about this yet. please try again with another querry")


# while True:
#     y=input()
#     responses(y)
#     if y=='end':
#         break
while (1):

    # Exception handling to handle
    # exceptions at the runtime
    try:

        # use the microphone as source for input.
        with sr.Microphone() as source2:

            # wait for a second to let the recognizer
            # adjust the energy threshold based on
            # the surrounding noise level
            r.adjust_for_ambient_noise(source2, duration=0.2)

            # listens for the user's input
            audio2 = r.listen(source2)

            # Using google to recognize audio
            MyText = r.recognize_google(audio2)
            MyText = MyText.lower()

            print(MyText)
            if MyText=='end program':
                SpeakText("ok boss see you later")
                break
            responses(MyText)


    except sr.RequestError as e:
        print("Could not request results; {0}".format(e))

    except sr.UnknownValueError:
        print("unknown error occurred")
