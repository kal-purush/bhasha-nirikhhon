"""WHOAH bot script."""
import atexit
import discord

import bot_functions as bf

# CONSTANTS
STATS_F = "stats.p"
#


def exit_handler():
    """Handle exit."""
    bf.save_stats(stats_count)


stats_count = bf.load_stats()
atexit.register(exit_handler)
client = discord.Client()

@client.event
async def on_message(message):
    """Function to handle message."""
    # Message sent by our bot
    if message.author == client.user:
        return

    msg_tup = bf.is_command(message.content)
    if msg_tup[0]:
        if msg_tup[1].find('hello') == 0:
            msg = 'Hello {0.author.mention}'.format(message)
            await client.send_message(message.channel, msg)

        elif msg_tup[1].find('repeat') == 0:
            msg = msg_tup[1][len('repeat'):]
            await client.send_message(message.channel, msg)

        elif msg_tup[1].find('stats_perc') == 0:
            await client.send_message(message.channel, bf.make_str_stats_perc(stats_count))

        elif msg_tup[1].find('stats') == 0:
            await client.send_message(message.channel, bf.make_str_stats(stats_count))

        elif msg_tup[1].find('weather') == 0:
            await client.send_message(message.channel, bf.get_wheater_condition(msg_tup[1]))

        elif msg_tup[1].find('fortune') == 0:
            await client.send_message(message.channel, 'Not implemented yet')

        elif msg_tup[1].find('help') == 0:
            await client.send_message(message.channel, bf.make_help_msg())

        else:
            await client.send_message(message.channel, bf.generate_whoah())

    else:
        # Stats stuff
        if message.author.mention not in stats_count:
            stats_count[message.author.mention] = 0
        stats_count[message.author.mention] += 1
        bf.save_stats(stats_count)


@client.event
async def on_ready():
    """Function when start up."""
    print('Logged in as')
    print(client.user.name)
    print(client.user.id)
    print('------')

client.run(bf.get_token())