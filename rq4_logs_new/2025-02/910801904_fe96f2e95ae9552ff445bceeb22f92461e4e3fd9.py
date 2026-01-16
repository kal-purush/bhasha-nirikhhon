import os
import datetime
import time
import requests
import wikipedia
import pyttsx3
import google.generativeai as genai
from transformers import AutoModelForSequenceClassification, AutoTokenizer
import torch
import torch.nn as nn
import re
from typing import List, Tuple, Dict


def clean_text(text: str) -> str:
    """Remove markdown formatting and clean text for speech."""
    text = re.sub(r'\*+([^\*]+)\*+', r'\1', text)
    text = re.sub(r'`([^`]+)`', r'\1', text)
    text = re.sub(r'\[([^\]]+)\]\([^\)]+\)', r'\1', text)
    text = ' '.join(text.split())
    return text


def wishMe():
    """Greet the user based on the time of day."""
    hour = datetime.datetime.now().hour
    if 0 <= hour < 12:
        return "Hello, Good Morning!"
    elif 12 <= hour < 18:
        return "Hello, Good Afternoon!"
    else:
        return "Hello, Good Evening!"


def get_weather(city_name: str, api_key: str) -> str:
    """Get weather information for a given city."""
    base_url = "https://api.openweathermap.org/data/2.5/weather?"
    complete_url = f"{base_url}appid={api_key}&q={city_name}"

    try:
        response = requests.get(complete_url)
        x = response.json()
        if x["cod"] != "404":
            y = x["main"]
            current_temperature = y["temp"]
            current_humidity = y["humidity"]
            z = x["weather"]
            weather_description = z[0]["description"]
            return (f"Temperature in Kelvin is {current_temperature}, "
                    f"Humidity is {current_humidity} percent, "
                    f"and the weather description is {weather_description}.")
        else:
            return "City not found"
    except Exception as e:
        return f"Error fetching weather: {str(e)}"


def get_top_news(api_key: str) -> List[str]:
    """Get top news headlines."""
    url = f'https://newsapi.org/v2/top-headlines?country=us&apiKey={api_key}'
    news_headlines = []

    try:
        response = requests.get(url)
        data = response.json()

        if data['status'] == 'ok' and 'articles' in data:
            articles = data['articles']
            for i, article in enumerate(articles[:10]):
                news_headlines.append(f"News {i + 1}: {article['title']}")
        return news_headlines
    except Exception as e:
        return [f"Error fetching news: {str(e)}"]


class EmotionalDialogueModel:
    def __init__(self):
        self.emotion_tokenizer = AutoTokenizer.from_pretrained("roberta-base")
        self.emotion_model = AutoModelForSequenceClassification.from_pretrained(
            "roberta-base", num_labels=7
        )
        self.conversation_history = []
        self.max_history = 5

    def detect_emotion(self, text: str) -> str:
        inputs = self.emotion_tokenizer(text, return_tensors="pt", truncation=True, max_length=512)
        outputs = self.emotion_model(**inputs)
        emotion_id = outputs.logits.argmax().item()
        emotions = ["joy", "sadness", "anger", "fear", "surprise", "disgust", "neutral"]
        return emotions[emotion_id]

    def generate_response(self, user_input: str, detected_emotion: str) -> str:
        response_templates = {
            "joy": "I'm glad you're feeling positive! ",
            "sadness": "I understand this might be difficult. ",
            "anger": "I can sense your frustration. Let's work through this together. ",
            "fear": "It's okay to feel anxious. I'm here to help. ",
            "surprise": "That's quite unexpected! ",
            "disgust": "I understand your discomfort. ",
            "neutral": ""
        }
        return response_templates.get(detected_emotion, "")


class HybridAssistant:
    def __init__(self, google_api_key: str, weather_api_key: str, news_api_key: str):
        self.engine = pyttsx3.init('sapi5')
        voices = self.engine.getProperty('voices')
        self.engine.setProperty('voice', voices[0].id)

        genai.configure(api_key=google_api_key)
        self.gemini_model = genai.GenerativeModel('gemini-pro')
        self.chat = self.gemini_model.start_chat(history=[])

        self.emotional_model = EmotionalDialogueModel()
        self.weather_api_key = weather_api_key
        self.news_api_key = news_api_key

    def speak(self, text: str) -> None:
        cleaned_text = clean_text(text)
        self.engine.say(cleaned_text)
        self.engine.runAndWait()

    def process_input(self, user_input: str) -> Tuple[str, str]:
        # Handle special commands first
        if 'weather' in user_input:
            self.speak("What's the city name?")
            city_name = input("City name: ").strip()
            response = get_weather(city_name, self.weather_api_key)
            return response, response

        elif 'time' in user_input:
            current_time = datetime.datetime.now().strftime("%H:%M:%S")
            response = f"The time is {current_time}"
            return response, response

        elif 'news' in user_input:
            headlines = get_top_news(self.news_api_key)
            response = "\n".join(headlines)
            return response, response

        elif 'wikipedia' in user_input:
            query = user_input.replace("wikipedia", "").strip()
            try:
                results = wikipedia.summary(query, sentences=3)
                return f"According to Wikipedia: {results}", f"According to Wikipedia: {results}"
            except Exception as e:
                return "Sorry, I couldn't fetch the information from Wikipedia.", "Sorry, I couldn't fetch the information from Wikipedia."

        # Default emotional + Gemini response
        emotion = self.emotional_model.detect_emotion(user_input)
        emotional_context = self.emotional_model.generate_response(user_input, emotion)

        display_response = ""
        speech_response = ""
        try:
            enhanced_prompt = f"Context: User's emotional state appears to be {emotion}. \
                Please respond appropriately to: {user_input}"

            response = self.chat.send_message(enhanced_prompt, stream=True)
            for chunk in response:
                if chunk.text:
                    display_response += chunk.text
                    speech_response += clean_text(chunk.text)
        except Exception as e:
            error_msg = f"Error getting AI response: {str(e)}"
            return error_msg, error_msg

        final_display = emotional_context + display_response
        final_speech = emotional_context + speech_response
        return final_display, final_speech


def run_hybrid_assistant():
    # API keys
    GOOGLE_API_KEY = 'AIzaSyDCoS5osHEnQguuOeniVuIrg47f_APzfc4'
    WEATHER_API_KEY = '7bcc14a012464257fb28ff3ee878bd39'
    NEWS_API_KEY = '1f83a0479f694f0091098c6bc25bcfe8'

    assistant = HybridAssistant(GOOGLE_API_KEY, WEATHER_API_KEY, NEWS_API_KEY)

    print("Loading your enhanced AI personal assistant...")
    assistant.speak("Loading your enhanced AI personal assistant")

    greeting = wishMe()
    print(greeting)
    assistant.speak(greeting)

    while True:
        try:
            user_input = input("You: ").strip().lower()

            if user_input == "exit":
                assistant.speak("Goodbye! Have a great day!")
                break

            display_response, speech_response = assistant.process_input(user_input)
            print("Assistant:", display_response)
            assistant.speak(speech_response)

        except Exception as e:
            print(f"Error: {str(e)}")
            assistant.speak("I encountered an error. Please try again.")


if __name__ == "__main__":
    run_hybrid_assistant()