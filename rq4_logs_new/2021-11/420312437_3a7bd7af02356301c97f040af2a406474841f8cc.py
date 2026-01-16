import discord
import random
import time

# insert bot token here
TOKEN = 'OTExMTU2Nzk5MDQwNzg2NDMy.YZdS5Q.3FSzC5QVQiesD8BF4HHgEIn23SQ'

client = discord.Client()

# functions start
autoreply = ['Hi',
             'Hello',
             'How are you?']

test = ["1", "2", "3"]
# functions end


@client.event
async def on_ready():
    # await client.change_presence(activity=discord.Game('Tite'))
    # bot activity,in this case he's watching a movie called movie
    await client.change_presence(activity=discord.Activity(type=discord.ActivityType.watching, name='Movie'))
    print('{0.user} is now online!'.format(client))  # bot status on discord


@client.event
async def on_message(message):
    username = str(message.author).split('#')[0]
    user_message = str(message.content)
    channel = str(message.channel.name)
    print(f'{username}: {user_message} ({channel})')

    if message.author == client.user:
        return

    if user_message.lower() == '!anywhere':
        await message.channel.send(f'This can be used anywhere!')
        return

    if message.author.id == 312524560224223233:  # server owner
        await message.channel.send('Hello master')
        return
    if message.author.id != 312524560224223233:  # if message sender is not server owner
        if user_message == '<@!911156799040786432>':
            await message.channel.send(random.choice(test))
        return

    ''' If you want the bot to send autoreplies in a specific channel
    if message.channel.name == 'channel_name':  # replace the channel name you want the bot to be in


    '''

    # autoreply lines

    slur = ['fuck']
    # autoreply end

    #    if message.author.id == 312524560224223233:
    #         if user_message == '<@!312533659880259584>':
    #             await message.channel.send('Bakla yang tanginang yan')
    #         if user_message == 'Hello':
    #             await message.channel.send('Hi pogi')
    #         if user_message == '<@!901304399777919077>':
    #             print(f'{username}: {user_message} ({channel})')
    #             await message.channel.send(autoreply)
    #         return

    #     if message.author.id == 690900203414224906:
    #         await message.channel.send('hey, just wanna ask something if kasama mo pa din sa room kanina si ash nung pauwi sya?')
    #         return

    #     # ID ni Jorg
    #     if message.author.id == 312533659880259584:
    #         await message.channel.send('Jakol')
    #         return

    #     if user_message == '<@!312533659880259584>':
    #         await message.channel.send('Bakla yang tanginang yan')
    #         return

    #     if user_message == '<@!901304399777919077>':
    #         print(f'{username}: {user_message} ({channel})')
    #         await message.channel.send(autoreply)
    #         return

    #     elif 'fuck' in user_message:
    #         print(f'{username}: {user_message} ({channel})')
    #         await message.channel.send('Tangina mo bakla!!!')
    #         return
    #     elif '<@!901304399777919077>' in user_message:
    #         if 'fuck' in user_message:
    #             print(f'{username}: {user_message} ({channel})')
    #             await message.channel.send('Tangina mo bakla!!!')
    #             return
    #     elif user_message.lower() == 'hello':
    #         await message.channel.send("Naol")
    #         return

    # if message.channel.name != '✉｜shitposting':
    #     if user_message.lower() == 'hello':
    #         await message.channel.send("Naolss")
    #         return
    #     if user_message.lower() == '<@!901304399777919077>':
    #         print(f'{username}: {user_message} ({channel})')
    #         await message.channel.send(f'Dili nalang ako magtalk')
    #         return

client.run(TOKEN)