from translate import Translator
from gtts import gTTS

text = input("Enter text in english : ")

translator=Translator(from_lang='en',to_lang="ml")

translated_text=translator.translate(text)

print(translated_text)