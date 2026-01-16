# bot.py
import os
import discord
from dotenv import load_dotenv
from discord.ext import commands


startup_extensions = ["embotcommands"]

load_dotenv()
TOKEN = os.getenv('DISCORD_TOKEN')
GUILD = os.getenv('DISCORD_GUILD')
CHANNEL = os.getenv('DISCORD_CHANNEL')



bot = commands.Bot(command_prefix='!')

@bot.event
async def on_ready():
    global guild 
    global beeroom
    global strikes

    guild = discord.utils.get(bot.guilds, name = GUILD)
    beeroom = discord.utils.get(guild.text_channels, name = 'beeroom')
    strikes = discord.utils.get(guild.text_channels, name = 'strikes')
    print(
        f'{bot.user} is connected to the following guild:\n'
        f'{guild.name}(id: {guild.id})'
    )
    print(bot.guilds)

# @bot.event
async def on_typing(channel, user, when):
    await beeroom.send(content = f"{user} is typing in {channel} at {when}")

# @bot.event
async def on_message(message):
    bannedwords = "playing with my"
    if message.author == bot.user:
        return
    if message.content.find(bannedwords) != -1:
        await beeroom.send(content = "STRIKE")

    await beeroom.send(content = f"{message.author.name} posted a messaged in {message.channel.name}")

@bot.event
async def on_raw_reaction_add(payload):
    print(payload)
    if payload.emoji.name == 'IDNAWN':
        # chatroom = discord.utils.get(guild.text_channels, id = payload.channel_id)
        chatroom = guild.get_channel(payload.channel_id)
        # emoter = str(guild.get_member(payload.user_id)).split('#',1)[0]
        await chatroom.send(content = f'{payload.member.name} : STRIKE')


@bot.command()
async def load(extension_name :str):
    bot.load_extension(extension_name)
    await bot.say("Extension Loaded")

if __name__ == "__main__":
    for extension in startup_extensions:
        bot.load_extension(extension)
bot.run(TOKEN)