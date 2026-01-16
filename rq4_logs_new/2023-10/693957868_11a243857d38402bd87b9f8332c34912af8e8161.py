import speech_recognition as sr
import docx
import time

def transcribe_speech_to_text():
    # Initialize the recognizer
    recognizer = sr.Recognizer()

    # Capture audio from the microphone
    with sr.Microphone() as source:
        print("Speak something...")
        recognizer.adjust_for_ambient_noise(source)  # Adjust for ambient noise
        audio = recognizer.listen(source)  # Listen to audio input

    try:
        # Recognize the speech using Google Web Speech API
        recognized_text = recognizer.recognize_google(audio)
        return recognized_text
    except sr.UnknownValueError:
        print("Google Web Speech API could not understand audio.")
    except sr.RequestError as e:
        print(f"Could not request results from Google Web Speech API; {e}")
    return None

def save_text_to_word(text):
    # Create a new Word document
    doc = docx.Document()
    
    # Add text to the document
    doc.add_paragraph(text)

    # Save the document to a file
    doc.save("speech_transcription.docx")

if __name__ == "__main__":
    # Step 1: Transcribe speech to text
    transcribed_text = transcribe_speech_to_text()

    if transcribed_text:
        print("Transcription:")
        print(transcribed_text)

        # Step 2: Save text to Word document
        save_text_to_word(transcribed_text)

        print("Transcription saved to 'speech_transcription.docx'")
    else:
        print("No transcription available.")