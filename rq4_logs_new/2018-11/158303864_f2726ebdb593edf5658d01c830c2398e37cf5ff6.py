import discord
from discord.ext import commands

bot = commands.Bot(command_prefix='$')

@bot.event
async def on_ready():
    print('Logged in as')
    print(bot.user.name)
    print(bot.user.id)
    print('------')

@bot.command()
async def greet(ctx):
    await ctx.send(":smiley: :wave: Hello, there!")

bot.run(NTE0MjIwOTc4ODU5OTMzNjk3.DtTZYA.cfsh8CNtptvA033iCVFzFNkeT48)