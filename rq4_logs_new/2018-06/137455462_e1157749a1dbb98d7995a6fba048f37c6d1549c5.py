import os
import configparser
import discord
import re

client = discord.Client()


@client.event
async def on_ready():
    # show status of bot & send initial message to bot channel
    print('Logged in as')
    print(client.user.name)
    print(client.user.id)
    print('------')
    send_message = "ゴーヤちゃん準備完了でち"
    channel = discord.utils.get(client.get_all_channels(), name='bot-echo')
    await client.send_message(channel, send_message)


@client.event
async def on_message(message):
    # send a response when goya-oji appears
    regex_oji = u"ゴーヤちゃんいるかな(\?|？)?(\^|＾){2}"
    is_oji = re.search(regex_oji, message.content)
    if is_oji:
        # except bot sender
        if client.user != message.author:
            react = "はいでち"
            await client.send_message(message.channel, react)


def get_key():
    # get a discord's api key from conf file at ./env dir. 
    conf = configparser.ConfigParser()
    base = os.path.dirname(os.path.abspath(__file__))
    conf_path = os.path.normpath(os.path.join(base, "./env/apikey.conf"))
    conf.read(conf_path, "UTF-8")
    key = conf.get("settings", "key")
    return key


def main():
    key = get_key()
    client.run(key)


main()