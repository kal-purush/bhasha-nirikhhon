import os
import time
import discord
import requests
import threading
from bs4 import BeautifulSoup
from dotenv import load_dotenv



load_dotenv()
TOKEN = os.getenv('DISCORD_TOKEN')

client = discord.Client()

price = 0
mm_url = "https://coinmarketcap.com/currencies/million/"
def ExtractMmPrice():
    global price
    while True:
        page = requests.get(mm_url)
        soup = BeautifulSoup(page.content, 'html.parser')
        price = soup.find_all("div", {"class": "priceValue"})[0].getText()
        print(price)
        time.sleep(60)

@client.event
async def on_message(message):
    if message.content.startswith('!price'):
        await message.reply(price, mention_author=True)

threading.Thread(target=ExtractMmPrice).start()
client.run(TOKEN)