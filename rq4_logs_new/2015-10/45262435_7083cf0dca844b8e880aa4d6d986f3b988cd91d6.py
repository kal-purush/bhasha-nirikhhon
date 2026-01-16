import telebot 
from telebot import types 
import time 
import requests
import re
import sys
import datetime
from collections import Counter
from xml.dom import minidom


TOKEN = '<TOKEN>'



usuarios = [line.rstrip('\n') for line in open('usuarios.txt')] 
hideBoard = types.ReplyKeyboardHide()  

 
bot = telebot.TeleBot(TOKEN) 


#Listener
def listener(messages):
    for m in messages:
        cid = m.chat.id
        if m.content_type == 'text': 
            if cid > 0:
                mensaje = str(m.chat.first_name) + " [" + str(cid) + "]: " + m.text
            else:
                mensaje = str(m.from_user.first_name) + "[" + str(cid) + "]: " + m.text 
            f = open('log.txt', 'a')
            f.write(mensaje + "\n")
            f.close()
            print (mensaje)
        elif m.content_type == 'new_chat_participant':
            if cid == grupo:
                        bot.send_message( cid, 'Hola' )
        elif m.content_type == 'left_chat_participant':
            if cid == grupo:
                    bot.send_message( cid, 'Adiós' )
                
                
bot.set_update_listener(listener) 


##########BOT START################
@bot.message_handler(commands=['start'])
def command_start(m):
    cid = m.chat.id
    if not str(cid) in usuarios: 
        usuarios.append(str(cid))
        aux = open( 'usuarios.txt', 'a') 
        aux.write( str(cid) + "\n")
        aux.close()
        bot.send_message( cid, "Bienvenido al bot Maldini")


##########XML################

def command_xml(m): 
    cid = m.chat.id
    f = open("live-soccer.xml",'r') #Archivo XML de ejemplo
    data = f.read()
    i = 0
    doc = minidom.parseString(data)
    for title in doc.getElementsByTagName('item'):
        title= doc.getElementsByTagName('title')[i].firstChild.nodeValue
        print (title)
        i +=1
        bot.send_message( cid, "ok")






#Peticiones
bot.polling(none_stop=True) 