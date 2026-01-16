import discord
from.discord.ext.commands import Bot
from discord.ext import commands
from discord.ext.commands.cooldowns import BucketType

import asyncio
import platform
import colorsys
import random
import os
import time

import discord
from.discord.ext import commands
import asyncio

bot=commands.Bot(command_prefix='/')
client.remove_command('help')

@bot.event
async def on_ready():
  print('The bot is ready!')
  print(bot.user.name)
  print(bot.user.id)
  
@bot.command()
async def fakeban():
  await bot.say('Dostal/a jsi ban! Ha jen si dělám standu :D')
  
@bot.command()
async def support():
  await bot.say('cms.megabot-support.webnode.cz')

@bot.command()
async def discord():
  await bot.say('https://discord.gg/9uer
N5z')

@bot.command()
async def hosting():
  await bot.say('****Zatím Žádný!****')
  
@bot.command()
async def discordbot():
  await bot.say('https://discord.gg/8dbfhWU')

client.run(os.getenv('NDkwOTQxOTY2Mjk3NTk1OTE0.DsJGrg.t0hx5ERaJrO4fvpZIkm4wGlft1g'))

