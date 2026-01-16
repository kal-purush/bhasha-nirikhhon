import speech_recognition as sr
import pyttsx3

# Initialize speech recognition and TTS
recognizer = sr.Recognizer()
engine = pyttsx3.init()

# Function to speak a response
def speak(text):
    engine.say(text)
    engine.runAndWait()

# Function to listen for audio and convert it to text
def listen():
    with sr.Microphone() as source:
        print("Listening...") #could change this to show potential commands or something
        audio = recognizer.listen(source)
        
        try:
            return recognizer.recognize_google(audio).lower()
        except sr.UnknownValueError:
            return "I couldn't understand what you said, please try again."
        except sr.RequestError:
            return "I'm having trouble connecting to the speech service."

# Function to handle the "search for" command
def handle_search_command(command):
    # Extract the search query from the command
    # Assuming the command starts with "search for"
    query = command.replace("search for", "", 1).strip()
    
    # Here you would add the logic to perform the search, for now we'll just repeat the query
    speak(f"Searching for {query}")

def main():
    while True:
        # Listen for commands
        command = listen()
        print(f"You said: {command}")

        # Check if the user said 'search for'
        if 'search for' in command: #include the other requests later
            handle_search_command(command)
        elif 'stop listening' in command:
            # Add a command to stop the assistant from listening
            speak("Goodbye!") #probably change this to a terminal input
            break
        else:
            speak("I'm sorry, I can only help with search requests right now.") #make this say invalid request or something later

if __name__ == "__main__":
    main()