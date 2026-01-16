import pyttsx3  # offline text to speech recognition library--> API
import speech_recognition as sr
import pyaudio  # with this we can use python for playing or recording audio
import webbrowser


tedith_engine = pyttsx3.init('sapi5')
voices = tedith_engine.getProperty('voices')  # got 2 voices
tedith_engine.setProperty('voices', voices[1].id)


def speak(audio):
    tedith_engine.say(audio)
    tedith_engine.runAndWait()


def greet():
    speak("Hey, I am TEDITH. How can I help you?")


def audioInput():
    r = sr.Recognizer()
    with sr.Microphone() as source:
        print("I'm all ears...")
        r.pause_threshold = 0.8
        audio = r.listen(source)

    try:
        print('Just a moment...')
        query = r.recognize_google(audio, language='en-in')
        print(f"You: {query}")

    except Exception as e:
        print('My bad!... Say that again, please...')
        return "None"
    return query

about_words=['about','introduce','describe','introduction']
mail_word=['mail','e mail','email']
count=0

if __name__ == '__main__':
    greet()

    while True:
        query = audioInput().lower()

        
        if any(c for c in about_words if c in query):
            about = 'Ina is a third-year undergraduate student in NIT Silchar with her major in Civil Engineering. She hails from Rewa, which is a former princely state in Madhya Pradesh. She is versatile and loves acquiring knowledge.'
            print(about)
            speak(about)

        elif 'contact' in query:
            speak("Opening Ina's website")
            webbrowser.open("https://inavashishtha.github.io/")
            
        elif ('skills') in query:
            skill='She is adept at Python(alognwith NumPy,Pandas,Matplotlib), C++, C, HTML, CSS,MS Excel,SQL, AutoCAD, QGIS. She is also a good orator and is competent in analytics and problem solving.'
            print(skill)
            speak(skill)

        elif 'hobbies' in query:
            hobby="Ina likes reading non-fiction books,is an amateur in writing poems and is an ardent movie buff. She listens to the ghazals of Jagjit Singh ji. She is a blend of old-school 90s kid and care-free Gen Z. Moreover, she's good at badminton and loves riding a bicycle."
            print(hobby)
            speak(hobby)

        elif 'linkedin' in query:
            speak("Opening Ina's LinkedIn Profile")
            webbrowser.open("https://www.linkedin.com/in/ina15")
        
        elif 'project' in query:
            p='''1.TEDITH-Ina's Assistant in March 2021,built by using python libraries,program works as a virtual assistant and tells the basic details about Ina.
            2. COVID-19 Vaccination in Shahdol,built in QGIS using the shapefiles in BHUVAN and government database,found out the distance between various health care centres and the nearest blood banks in Shahdol District.'''
            print(p)
            speak(p)

        elif 'internship' in query:
            i="Industrial Training at Lines International Jaipur, Business Management Intern at Rakesh Trivedi Surveyor & Loss Assessor, Environmental Resouce Intern at RES Sidhi"
            print(i)
            speak(i)

        elif 'branch' in query:
            branch='Civil Engineering'
            print(branch)
            speak(branch)

        elif 'soft skills' in query:
            ss="Public Speaking,Creativity, Leadership, Optimism, Management,Team Work, Detail-oreinted, Good Listener"
            print(ss)
            speak(ss)

        elif any(c for c in mail_word if c in query ):
            m='ina_ug@civil.nits.ac.in'
            print(m)
            speak(m)

        elif 'summary' in query:
            summary="Seeking an opportunity to build a career that would intrinsically help me achieve greater practical excellence and learn to apply technical and sustainable solutions to various problems."
            print(summary)
            speak(summary)

        elif 'achievements'in query:
            a="HackerRank: 5-star- Gold Badge in C++ and Python, 4-star Silver Badge in Problem Solving,Qualified National Engineering Olympiad 3.0 (Round 1),Honorable Mention in Model United Nation: UNGA NIT Silchar,Winner of Smart City module (PPT on urban planning) in Tecnoesis NIT Silchar,Winner of various essay poem writing extempore debate,Letter of Appreciation by Hon. Minister of HRD Smriti Z. Irani."
            print(a)
            speak(a)
        
        elif 'extra curricular' in query:
            ec="Technical Secretary, Civil Engineering Society, NIT Silchar"

        elif 'father' in query:
            f='Rakesh Trivedi'
            print(f)
            speak(f)

        elif 'mother' in query:
            mom='Maneesha Trivedi'
            print(mom)
            speak(mom)

        elif ('meaning') in query:
            print('Tony Even Dead Is The Hero')
            speak('Tony Even Dead Is The Hero')
        
        else:
            speak('Try something else...')
            count=count+1
            
            if count==5:
                break
    print('Thanks for using.')
    speak('Thanks for using.')            
