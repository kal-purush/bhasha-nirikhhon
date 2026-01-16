import os

import discord
import settings
import numpy as np
from scipy.integrate import quad
from math import sqrt
import random
import re

TOKEN = settings.TOKEN

client = discord.Client()

def parseStr(diceStr):
    diceStr = diceStr.split(" ")
    repeats = 1
    if len(diceStr) > 1:
        repeats = int(diceStr[0])
    diceStr = diceStr[len(diceStr) - 1]
    diceStr = ' ' + diceStr
    ans = ''
    for i in range(0,repeats):
        tempDiceStr = diceStr
        cubesStr = ''
        x = re.findall('[ \+\-\*\/][\+\-]d[0-9]*[^\+\-\/\*\n]',tempDiceStr)
        for current in x:
            current = current[1:]
            replaceBy = 0
            rolling = current[1:].split('d')
            a,cubesStr = rollDice(int(rolling[1]),2,cubesStr)
            if current[0] == '+':
                replaceBy = max(a)
            else:
                replaceBy = min(a)
            tempDiceStr = tempDiceStr.replace(current, str(replaceBy), 1)
        x = re.findall('[0-9]*d[0-9][^\+\-\/\*\n]',tempDiceStr)
        for current in x:
            replaceBy = 0
            rolling = current.split('d')
            if(rolling[0] == ''):
                rolling[0] = '1'
            delim = '+'
            diceRes,cubesStr = rollDice(int(rolling[1]),int(rolling[0]),cubesStr)
            replaceBy = delim.join(map(str,diceRes))
            tempDiceStr = tempDiceStr.replace(current, str(replaceBy), 1)
        ans += cubesStr + '` Result: `' + str(eval(tempDiceStr)) + '\n'
    return ans

def rollDice(num, k, cubesStr):
    val = {}
    realP = 0
    mu = (num + 1) / 2
    sig = mu / 3
    for n in range(1,num+1):
        val[n],_ = quad(lambda x : 1/(sqrt(2*np.pi) * sig) * np.e**(-(x-mu)**2/(2*sig**2)), n-0.5, n+0.5)
        realP += val[n]
    realP = 1 - realP
    realP /= num
    for n in range(1,num+1):
        val[n] += realP
    retVal = random.choices(list(range(1,num+1)),val,k=k)
    cubesStr += str(retVal)
    return retVal, cubesStr


@client.event
async def on_ready():
    print(f'{client.user} has connected to Discord!')

@client.event
async def on_message(message):
    if message.author == client.user:
        return

    if message.content.startswith('!roll '):
        diceStr = message.content[6:]
        try:
            roll = parseStr(diceStr)
            await message.channel.send(f'USING MY DIC(<:wonka:458194762290167819>)e:\n`{roll}`')
        except:
            await message.channel.send("Expression down!")
        

client.run(TOKEN)