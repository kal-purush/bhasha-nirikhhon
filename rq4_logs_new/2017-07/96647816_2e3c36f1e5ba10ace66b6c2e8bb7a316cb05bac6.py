import speech_recognition as sr


r = sr.Recognizer()
with sr.Microphone(sample_rate=48000) as source:
    r.adjust_for_ambient_noise(source)
    print('Please say your input')
    audio = r.listen(source)
    
    try:
        text = r.recognize_google(audio)
        print "you said: {}".format(text)
    except sr.UnknownValueError:
        print 'Google Speech Recognition could not understand audio'
    except sr.RequestError as e:
        print 'Could not request results from Google Speech Recognition service; {}'.format(e)
    # print r.recognize_google_cloud(audio)