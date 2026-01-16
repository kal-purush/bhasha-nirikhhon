# manage your computer from your phone via telegram bot 
# by Filippo Renai
# sorry my bad english,for each question contact me on telegram: @Filippo1996

import telepot
import webbrowser
import os
import subprocess

def on_chat_message(msg):
    content_type, chat_type, chat_id = telepot.glance(msg)

    if content_type == 'text':
        global op
        info = msg['from']
        user = info['username']

        if user == 'your telegram's username':
            if msg['text'] == '/start': #start
                bot.sendMessage(chat_id, 'hello :D')

            elif msg['text'] == '/1': #open web mode
                op = 1
                bot.sendMessage(chat_id, 'now you can paste the link of the web page that you want hope')

            elif msg['text'] == '/2': #close web mode
                op = 0
                bot.sendMessage(chat_id, 'close web mode,you can't open any link')

            elif msg['text'] == '/3': #close chrome
                os.system("taskkill /im chrome.exe /f")
                bot.sendMessage(chat_id, 'chrome closed')

            elif msg['text'] == '/4': #turn off pc
                bot.sendMessage(chat_id, 'pc turn off')
                os.system('shutdown /s')

            elif op == 1: #now you can paste the url of any link that you want open
                webbrowser.open(msg['text'])
                bot.sendMessage(chat_id, 'link open')

            elif op == 0: #you have to turn on the web mode for open the link
                bot.sendMessage(chat_id, 'you have to turn on the web mode for open the link')

            else: #wrong type
                bot.sendMessage(chat_id, 'wrong type')
        else:
            bot.sendMessage(chat_id, 'you shouldn't be here: ' + user)

op = 0
TOKEN = "your telegram bot token"
bot = telepot.Bot(TOKEN)
bot.message_loop(on_chat_message)

print('Listening ...')

import time
while 1:
    time.sleep(10)