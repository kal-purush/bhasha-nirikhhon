import os
import discord
import openai

openai.api_key = ""

TOKEN = ""
client = discord.Client()

@client.event
async def on_ready():
    print("Were ready to rumble!")

@client.event
async def on_message(message):
    if message.author == client.user:
        return
    if message[1:3] == ";; ":
        d