import pyowm
import telebot
from datetime import datetime
import time

# Your OpenWeatherMap API key
OWM_API_KEY = 'f6ce8cb27ec1b4a4fb183640385f4335'

# Your Telegram Bot API token
TELEGRAM_API_TOKEN = '6910049657:AAH4qa_oK3jnoJxDxWAD_5XaPRhxtBCJtuE'

# Initialize OpenWeatherMap client
owm = pyowm.OWM(OWM_API_KEY)

# Initialize Telegram bot
bot = telebot.TeleBot(TELEGRAM_API_TOKEN)

# Function to send weather updates
def send_weather_update():
    # Get current weather for your location (replace with your location)
    observation = owm.weather_at_place("Your_Location")
    w = observation.get_weather()

    # Get weather information
    weather_status = w.get_status()
    temperature = w.get_temperature('celsius')['temp']

    # Compose the message
    message = f"Good morning! Weather: {weather_status}, Temperature: {temperature}°C"

    # Send the message to your Telegram
    bot.send_message(YOUR_CHAT_ID, message)

# Schedule the daily weather update at 8 am
while True:
    now = datetime.now()
    if now.hour == 8 and now.minute == 0:
        send_weather_update()
    time.sleep(60)  # Sleep for 1 minute

# Run the bot
bot.polling()