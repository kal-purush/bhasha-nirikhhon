#!/usr/bin/python
# -*- coding: utf-8 -*-
import telebot
from telebot import types
from telebot import util
import sys
import os
import requests
import urllib
import urllib2
import redis
import requests as req
reload(sys)
sys.setdefaultencoding("utf-8")
TOKEN = '382407045:AAE42AmH9zX9-3dsbcrCWD4NnY4BswP8c2Y'
channel = '@tgMember'
sudo = '158955285'
bot = telebot.TeleBot(TOKEN)
redis = redis.StrictRedis(host='localhost', port=6379, db=0)
db = "https://api.telegram.org/bot{}/getMe?".format(TOKEN)
f = "\n \033[01;30m Bot Firstname: {} \033[0m".format(bot.get_me().first_name)
u = "\n \033[01;34m Bot Username: {} \033[0m".format(bot.get_me().username)
i = "\n \033[01;32m Bot ID: {} \033[0m".format(bot.get_me().id)
c = "\n \033[01;31m Bot Is Online Now! \033[0m"
b = "\n \033[01;35m Channel: @tgMember\033[0m"
print(f + u + i + c + b)
@bot.message_handler(commands=['start'])
def start(m):
 try:
  rank = bot.get_chat_member(channel,m.from_user.id).status
  print rank
  if rank == "creator" or rank == "administrator" or rank == "member":
    markup = types.InlineKeyboardMarkup()
    c = types.InlineKeyboardButton("درباره",callback_data='about')
    markup.add(c)
    redis.sadd('members',m.from_user.id)
    bot.send_message(m.chat.id, "به ربات لوگو استیکر خوش امدید\nکار کردن با این بات خیلی راحته فقط کافیه متنت وارد کنی تا  لوگو مورد نظرت انتخاب کنی", disable_notification=True, reply_markup=markup, parse_mode='Markdown')
  else:
    markup = types.InlineKeyboardMarkup()
    ch = types.InlineKeyboardButton('عضومیشوم',url='https://telegram.me/joinchat/AAAAAEC-CTpnz_0J0dplCg')
    markup.add(ch)
    bot.send_message(m.chat.id,'سلام\nبرای استفاده از قابلیت های ربات و مطلع شدن ازاپدیت ها و...\n باید در کانال عضوشودیدو مجدد /start را بزنید',reply_markup=markup)
 except IndexError:
    markup = types.InlineKeyboardMarkup()
    c = types.InlineKeyboardButton("درباره",callback_data='about')
    markup.add(c)
    redis.sadd('members',m.from_user.id)
    bot.send_message(m.chat.id, "به ربات لوگو استیکر خوش امدید\nکار کردن با این بات خیلی راحته فقط کافیه متنت وارد کنی تا  لوگو مورد نظرت انتخاب کنی", disable_notification=True, reply_markup=markup, parse_mode='Markdown')
	
@bot.message_handler(commands=['bc'])
def clac(m):
    if str(m.from_user.id) == sudo :
        text = m.text.replace("/bc ","")
        rd = redis.smembers('members')
        for id in rd:
            try:
                bot.send_message(id, "{}".format(text), parse_mode="Markdown")
            except:
                redis.srem('members', id)
				
@bot.message_handler(commands=['panel'])
def panel(m):
    if str(m.from_user.id) == sudo :
     markup = types.InlineKeyboardMarkup()
     c = types.InlineKeyboardButton("امار",callback_data='amar')
     markup.add(c)
     bot.send_message(m.chat.id, "پنل مدریتی", reply_markup=markup, parse_mode='Markdown')
	 
@bot.message_handler(commands=['fwdall'])
def fwdall(m):
    if str(m.from_user.id) == sudo :
        if m.reply_to_message:
            mid = m.reply_to_message.message_id
            ids = redis.smembers('members')
            for id in ids:
                try:
                    bot.forward_message(id,m.chat.id,mid)
                except:
                    redis.srem('members',id)	
@bot.message_handler(content_types=['text'])
def logosticker(m):
    rank = bot.get_chat_member(channel,m.from_user.id).status
    print rank
    if rank == "creator" or rank == "administrator" or rank == "member":
        redis.hset("logosticker","{}".format(m.from_user.id),"{}".format(m.text))
        markup = types.InlineKeyboardMarkup()
        c = types.InlineKeyboardButton("طرح ساده(انگلیسی)",callback_data='1')
        v = types.InlineKeyboardButton("طرح ساده(انگلیسی)",callback_data='2')
        markup.add(c,v)
        q = types.InlineKeyboardButton("طرح ساده(انگلیسی)",callback_data='3')
        w = types.InlineKeyboardButton("طرح ساده(انگلیسی)",callback_data='4')
        markup.add(q,w)
        e = types.InlineKeyboardButton("طرح ساده(انگلیسی)",callback_data='5')
        r = types.InlineKeyboardButton("طرح ساده(انگلیسی)",callback_data='6')
        markup.add(e,r)
        t = types.InlineKeyboardButton("طرح ساده(فارسی)",callback_data='7')
        y = types.InlineKeyboardButton("طرح ساده(فارسی)",callback_data='8')
        markup.add(t,y)
        u = types.InlineKeyboardButton("طرح ساده(فارسی)",callback_data='9')
        z = types.InlineKeyboardButton("طرح ساده(فارسی)",callback_data='10')
        markup.add(u,z)
        hg = types.InlineKeyboardButton("طرح ساده(فارسی)",callback_data='11')
        gh = types.InlineKeyboardButton("طرح ساده(فارسی)",callback_data='12')
        markup.add(hg,gh)
        kj = types.InlineKeyboardButton("1️⃣",callback_data='s1')
        jh = types.InlineKeyboardButton("صفه بعد▶️",callback_data='next')
        markup.add(kj,jh)
        bot.send_message(m.chat.id, "لطفا طرح خود را انتخاب کنید:", disable_notification=True, reply_markup=markup, parse_mode="Markdown")
    else:
        markup = types.InlineKeyboardMarkup()
        ch = types.InlineKeyboardButton('عضومیشوم',url='https://telegram.me/joinchat/AAAAAEC-CTpnz_0J0dplCg')
        markup.add(ch)
        bot.send_message(m.chat.id,'سلام\nبرای استفاده از قابلیت های ربات و مطلع شدن ازاپدیت ها و...\n باید در کانال عضوشودیدو مجدد /start را بزنید',reply_markup=markup)
@bot.callback_query_handler(func=lambda call: True)
def callback(call):
  if call.message: 
     if call.data == "1":
        text = redis.hget("logosticker","{}".format(call.from_user.id))
        link = "http://api.monsterbot.ir/pic?text={}&color=gray&font=1".format(text)
        urllib.urlretrieve(link, "stick.png")
        file1 = open('stick.png', 'rb')
        bot.send_sticker(call.message.chat.id,file1)
	os.remove('stick.png')
  if call.message: 
     if call.data == "2":
        text = redis.hget("logosticker","{}".format(call.from_user.id))
        link = "http://api.monsterbot.ir/pic?text={}&color=gray&font=2&fsize=250".format(text)
        urllib.urlretrieve(link, "stick.png")
        file1 = open('stick.png', 'rb')
        bot.send_sticker(call.message.chat.id,file1)
	os.remove('stick.png')
  if call.message: 
     if call.data == "3":
        text = redis.hget("logosticker","{}".format(call.from_user.id))
        link = "http://api.monsterbot.ir/pic?text={}&color=gray&font=BlackWidow&fsize=300".format(text)
        urllib.urlretrieve(link, "stick.png")
        file1 = open('stick.png', 'rb')
        bot.send_sticker(call.message.chat.id,file1)
	os.remove('stick.png')
  if call.message: 
     if call.data == "4":
        text = redis.hget("logosticker","{}".format(call.from_user.id))
        link = "http://api.monsterbot.ir/pic?text={}&color=gray&font=trutagem&fsize=200".format(text)
        urllib.urlretrieve(link, "stick.png")
        file1 = open('stick.png', 'rb')
        bot.send_sticker(call.message.chat.id,file1)
	os.remove('stick.png')
  if call.message: 
     if call.data == "5":
        text = redis.hget("logosticker","{}".format(call.from_user.id))
        link = "http://api.monsterbot.ir/pic?text={}&color=gray&font=xtrusion&fsize=200".format(text)
        urllib.urlretrieve(link, "stick.png")
        file1 = open('stick.png', 'rb')
        bot.send_sticker(call.message.chat.id,file1)
	os.remove('stick.png')
  if call.message: 
     if call.data == "6":
        text = redis.hget("logosticker","{}".format(call.from_user.id))
        link = "http://api.monsterbot.ir/pic/?text={}&color=gray&font=Cristalid&fsize=200".format(text)
        urllib.urlretrieve(link, "stick.png")
        file1 = open('stick.png', 'rb')
        bot.send_sticker(call.message.chat.id,file1)
	os.remove('stick.png')
  if call.message: 
     if call.data == "7":
        text = redis.hget("logosticker","{}".format(call.from_user.id))
        link = "http://api.monsterbot.ir/pic/?color=gray&font=IranNastaliq&text={}&fsize=200".format(text)
        urllib.urlretrieve(link, "stick.png")
        file1 = open('stick.png', 'rb')
        bot.send_sticker(call.message.chat.id,file1)
	os.remove('stick.png')
  if call.message: 
     if call.data == "8":
        text = redis.hget("logosticker","{}".format(call.from_user.id))
        link = "http://api.monsterbot.ir/pic/?color=gray&font=ftne&text={}&fsize=200".format(text)
        urllib.urlretrieve(link, "stick.png")
        file1 = open('stick.png', 'rb')
        bot.send_sticker(call.message.chat.id,file1)
	os.remove('stick.png')
  if call.message: 
     if call.data == "9":
        text = redis.hget("logosticker","{}".format(call.from_user.id))
        link = "http://api.monsterbot.ir/pic/?color=gray&font=Maktoob&text={}&fsize=300".format(text)
        urllib.urlretrieve(link, "stick.png")
        file1 = open('stick.png', 'rb')
        bot.send_sticker(call.message.chat.id,file1)
	os.remove('stick.png')
  if call.message: 
     if call.data == "10":
        text = redis.hget("logosticker","{}".format(call.from_user.id))
        link = "http://api.monsterbot.ir/pic/?color=gray&font=dara-form&text={}&fsize=200".format(text)
        urllib.urlretrieve(link, "stick.png")
        file1 = open('stick.png', 'rb')
        bot.send_sticker(call.message.chat.id,file1)
	os.remove('stick.png')
  if call.message: 
     if call.data == "11":
        text = redis.hget("logosticker","{}".format(call.from_user.id))
        link = "http://api.monsterbot.ir/pic/?color=gray&font=SOGAND&text={}&fsize=250".format(text)
        urllib.urlretrieve(link, "stick.png")
        file1 = open('stick.png', 'rb')
        bot.send_sticker(call.message.chat.id,file1)
	os.remove('stick.png')
  if call.message: 
     if call.data == "12":
        text = redis.hget("logosticker","{}".format(call.from_user.id))
        link = "http://api.monsterbot.ir/pic/?color=gray&font=farsi_Shadow&text={}&fsize=250".format(text)
        urllib.urlretrieve(link, "stick.png")
        file1 = open('stick.png', 'rb')
        bot.send_sticker(call.message.chat.id,file1)
	os.remove('stick.png')
  if call.message: 
     if call.data == "13":
        text = redis.hget("logosticker","{}".format(call.from_user.id))
        link = "http://api.monsterbot.ir/pic/?text={}&color=black&font=Steamy&fsize=150&bg=games5".format(text)
        urllib.urlretrieve(link, "stick.png")
        file1 = open('stick.png', 'rb')
        bot.send_sticker(call.message.chat.id,file1)
  if call.message: 
     if call.data == "14":
        text = redis.hget("logosticker","{}".format(call.from_user.id))
        link = "http://api.monsterbot.ir/pic/?text={}&color=red&font=Assassin&fsize=60&bg=game".format(text)
        urllib.urlretrieve(link, "stick.png")
        file1 = open('stick.png', 'rb')
        bot.send_sticker(call.message.chat.id,file1)
  if call.message: 
     if call.data == "15":
        text = redis.hget("logosticker","{}".format(call.from_user.id))
        link = "http://api.monsterbot.ir/pic/?text={}&color=red&font=Hitman&fsize=200&bg=game2".format(text)
        urllib.urlretrieve(link, "stick.png")
        file1 = open('stick.png', 'rb')
        bot.send_sticker(call.message.chat.id,file1)
  if call.message: 
     if call.data == "16":
        text = redis.hget("logosticker","{}".format(call.from_user.id))
        link = "http://api.monsterbot.ir/pic/?text={}&color=red&font=Hitman&fsize=200&bg=game3".format(text)
        urllib.urlretrieve(link, "stick.png")
        file1 = open('stick.png', 'rb')
        bot.send_sticker(call.message.chat.id,file1)
  if call.message: 
     if call.data == "17":
        text = redis.hget("logosticker","{}".format(call.from_user.id))
        link = "http://api.monsterbot.ir/pic/?text={}&color=black&font=Steamy&fsize=200&bg=games".format(text)
        urllib.urlretrieve(link, "stick.png")
        file1 = open('stick.png', 'rb')
        bot.send_sticker(call.message.chat.id,file1)
  if call.message: 
     if call.data == "18":
        text = redis.hget("logosticker","{}".format(call.from_user.id))
        link = "http://api.monsterbot.ir/pic/?text={}&color=black&font=Hitman&fsize=250&bg=batman".format(text)
        urllib.urlretrieve(link, "stick.png")
        file1 = open('stick.png', 'rb')
        bot.send_sticker(call.message.chat.id,file1)
  if call.message: 
     if call.data == "19":
        text = redis.hget("logosticker","{}".format(call.from_user.id))
        link = "http://api.monsterbot.ir/pic/?text={}&color=black&font=Steamy&fsize=150&bg=sIFONOT".format(text)
        urllib.urlretrieve(link, "stick.png")
        file1 = open('stick.png', 'rb')
        bot.send_sticker(call.message.chat.id,file1)
  if call.message: 
     if call.data == "20":
        text = redis.hget("logosticker","{}".format(call.from_user.id))
        link = "http://api.monsterbot.ir/pic/?text={}&color=black&font=Steamy&bg=counter-strike&fsize=80".format(text)
        urllib.urlretrieve(link, "stick.png")
        file1 = open('stick.png', 'rb')
        bot.send_sticker(call.message.chat.id,file1)
  if call.message: 
     if call.data == "21":
        text = redis.hget("logosticker","{}".format(call.from_user.id))
        link = "http://api.monsterbot.ir/pic/?text={}&color=white&font=royale&fsize=100&bg=clash-royale".format(text)
        urllib.urlretrieve(link, "stick.png")
        file1 = open('stick.png', 'rb')
        bot.send_sticker(call.message.chat.id,file1)
  if call.message: 
     if call.data == "22":
        text = redis.hget("logosticker","{}".format(call.from_user.id))
        link = "http://api.monsterbot.ir/pic/?text={}&color=white&font=royale&fsize=100&bg=clash-royale2".format(text)
        urllib.urlretrieve(link, "stick.png")
        file1 = open('stick.png', 'rb')
        bot.send_sticker(call.message.chat.id,file1)
  if call.message: 
     if call.data == "23":
        text = redis.hget("logosticker","{}".format(call.from_user.id))
        link = "http://api.monsterbot.ir/pic/?text={}&color=white&font=mk&fsize=100&bg=raiden(beta)".format(text)
        urllib.urlretrieve(link, "stick.png")
        file1 = open('stick.png', 'rb')
        bot.send_sticker(call.message.chat.id,file1)
  if call.message: 
     if call.data == "24":
        text = redis.hget("logosticker","{}".format(call.from_user.id))
        link = "http://api.monsterbot.ir/pic/?text={}&color=white&font=mk&fsize=100&bg=sub-zero(beta)".format(text)
        urllib.urlretrieve(link, "stick.png")
        file1 = open('stick.png', 'rb')
        bot.send_sticker(call.message.chat.id,file1)
  if call.message: 
     if call.data == "25":
        text = redis.hget("logosticker","{}".format(call.from_user.id))
        link = "http://api.monsterbot.ir/pic/?text={}&color=black&font=Minecrafter.Alt&fsize=50&bg=Minecrafter2".format(text)
        urllib.urlretrieve(link, "stick.png")
        file1 = open('stick.png', 'rb')
        bot.send_sticker(call.message.chat.id,file1)
  if call.message: 
     if call.data == "26":
        text = redis.hget("logosticker","{}".format(call.from_user.id))
        link = "http://api.monsterbot.ir/pic/?text={}&color=white&font=mk&fsize=100&bg=batraiden(beta)".format(text)
        urllib.urlretrieve(link, "stick.png")
        file1 = open('stick.png', 'rb')
        bot.send_sticker(call.message.chat.id,file1)
  if call.message: 
     if call.data == "27":
        text = redis.hget("logosticker","{}".format(call.from_user.id))
        link = "http://api.monsterbot.ir/pic/?text={}&color=Gray&font=Steamy&fsize=130&bg=cs".format(text)
        urllib.urlretrieve(link, "stick.png")
        file1 = open('stick.png', 'rb')
        bot.send_sticker(call.message.chat.id,file1)
  if call.message: 
     if call.data == "27":
        text = redis.hget("logosticker","{}".format(call.from_user.id))
        link = "http://api.monsterbot.ir/pic/?text={}&color=Gray&font=Steamy&fsize=130&bg=cs".format(text)
        urllib.urlretrieve(link, "stick.png")
        file1 = open('stick.png', 'rb')
        bot.send_sticker(call.message.chat.id,file1)
  if call.message: 
     if call.data == "28":
        text = redis.hget("logosticker","{}".format(call.from_user.id))
        link = "http://api.monsterbot.ir/pic/?text={}&color=Gray&font=Steamy&fsize=130&bg=cs".format(text)
        urllib.urlretrieve(link, "stick.png")
        file1 = open('stick.png', 'rb')
        bot.send_sticker(call.message.chat.id,file1)
  if call.message: 
     if call.data == "29":
        text = redis.hget("logosticker","{}".format(call.from_user.id))
        link = "http://api.monsterbot.ir/pic/?text={}&color=red&font=Steamy&fsize=80&bg=shakh".format(text)
        urllib.urlretrieve(link, "stick.png")
        file1 = open('stick.png', 'rb')
        bot.send_sticker(call.message.chat.id,file1)
  if call.message: 
     if call.data == "30":
        text = redis.hget("logosticker","{}".format(call.from_user.id))
        link = "http://api.monsterbot.ir/pic/?text={}&color=red&font=Steamy&fsize=80&bg=shakh2".format(text)
        urllib.urlretrieve(link, "stick.png")
        file1 = open('stick.png', 'rb')
        bot.send_sticker(call.message.chat.id,file1)
  if call.message: 
     if call.data == "31":
        text = redis.hget("logosticker","{}".format(call.from_user.id))
        link = "http://api.monsterbot.ir/pic/?text={}&color=red&font=Steamy&fsize=100&bg=wallpaperhds".format(text)
        urllib.urlretrieve(link, "stick.png")
        file1 = open('stick.png', 'rb')
        bot.send_sticker(call.message.chat.id,file1)
  if call.message: 
     if call.data == "32":
        text = redis.hget("logosticker","{}".format(call.from_user.id))
        link = "http://api.monsterbot.ir/pic/?text={}&color=black&font=Steamy&fsize=150&bg=joker".format(text)
        urllib.urlretrieve(link, "stick.png")
        file1 = open('stick.png', 'rb')
        bot.send_sticker(call.message.chat.id,file1)
  if call.message: 
     if call.data == "33":
        text = redis.hget("logosticker","{}".format(call.from_user.id))
        link = "http://api.monsterbot.ir/pic/?text={}&color=black&font=Steamy&fsize=80&bg=shakh3".format(text)
        urllib.urlretrieve(link, "stick.png")
        file1 = open('stick.png', 'rb')
        bot.send_sticker(call.message.chat.id,file1)
  if call.message: 
     if call.data == "34":
        text = redis.hget("logosticker","{}".format(call.from_user.id))
        link = "http://api.monsterbot.ir/pic/?text={}&color=black&font=Steamy&fsize=80&bg=shakh4".format(text)
        urllib.urlretrieve(link, "stick.png")
        file1 = open('stick.png', 'rb')
        bot.send_sticker(call.message.chat.id,file1)
  if call.message: 
     if call.data == "35":
        text = redis.hget("logosticker","{}".format(call.from_user.id))
        link = "http://api.monsterbot.ir/pic/?text={}&color=white&font=Assassin&fsize=80&bg=shakh5".format(text)
        urllib.urlretrieve(link, "stick.png")
        file1 = open('stick.png', 'rb')
        bot.send_sticker(call.message.chat.id,file1)
  if call.message: 
     if call.data == "36":
        text = redis.hget("logosticker","{}".format(call.from_user.id))
        link = "http://api.monsterbot.ir/pic/?text={}&color=white&font=Assassin&fsize=80&bg=shakh6".format(text)
        urllib.urlretrieve(link, "stick.png")
        file1 = open('stick.png', 'rb')
        bot.send_sticker(call.message.chat.id,file1)
  if call.message: 
     if call.data == "37":
        text = redis.hget("logosticker","{}".format(call.from_user.id))
        link = "http://api.monsterbot.ir/pic/?text={}&color=white&y=376&font=ANGEL&fsize=120&bg=shakh10".format(text)
        urllib.urlretrieve(link, "stick.png")
        file1 = open('stick.png', 'rb')
        bot.send_sticker(call.message.chat.id,file1)
  if call.message: 
     if call.data == "38":
        text = redis.hget("logosticker","{}".format(call.from_user.id))
        link = "http://api.monsterbot.ir/pic/?text={}&color=white&y=376&font=ANGEL&fsize=120&bg=shakh10".format(text)
        urllib.urlretrieve(link, "stick.png")
        file1 = open('stick.png', 'rb')
        bot.send_sticker(call.message.chat.id,file1)
  if call.message: 
     if call.data == "39":
        text = redis.hget("logosticker","{}".format(call.from_user.id))
        link = "http://api.monsterbot.ir/pic/?text={}&color=white&y=250&font=28&fsize=60&bg=shakh20".format(text)
        urllib.urlretrieve(link, "stick.png")
        file1 = open('stick.png', 'rb')
        bot.send_sticker(call.message.chat.id,file1)
  if call.message: 
     if call.data == "40":
        text = redis.hget("logosticker","{}".format(call.from_user.id))
        link = "http://api.monsterbot.ir/pic/?text={}&color=black&y=135&x=0&font=28&fsize=70&bg=shakh31".format(text)
        urllib.urlretrieve(link, "stick.png")
        file1 = open('stick.png', 'rb')
        bot.send_sticker(call.message.chat.id,file1)
  if call.message: 
     if call.data == "41":
        text = redis.hget("logosticker","{}".format(call.from_user.id))
        link = "http://api.monsterbot.ir/pic/?text={}&color=grey&y=185&font=Steamy&fsize=80&bg=shakh36".format(text)
        urllib.urlretrieve(link, "stick.png")
        file1 = open('stick.png', 'rb')
        bot.send_sticker(call.message.chat.id,file1)
  if call.message: 
     if call.data == "42":
        text = redis.hget("logosticker","{}".format(call.from_user.id))
        link = "http://api.monsterbot.ir/pic/?text={}&y=175&color=white&font=Steamy&fsize=70&bg=shakh37".format(text)
        urllib.urlretrieve(link, "stick.png")
        file1 = open('stick.png', 'rb')
        bot.send_sticker(call.message.chat.id,file1)
  if call.message: 
     if call.data == "43":
        text = redis.hget("logosticker","{}".format(call.from_user.id))
        link = "http://api.monsterbot.ir/pic/?text={}&color=white&font=28&fsize=50&bg=logo31&angel=-0&y=245".format(text)
        urllib.urlretrieve(link, "stick.png")
        file1 = open('stick.png', 'rb')
        bot.send_sticker(call.message.chat.id,file1)
  if call.message: 
     if call.data == "44":
        text = redis.hget("logosticker","{}".format(call.from_user.id))
        link = "http://api.monsterbot.ir/pic/?text={}&color=white&y=317&x=0&font=28&fsize=90&bg=shakh25".format(text)
        urllib.urlretrieve(link, "stick.png")
        file1 = open('stick.png', 'rb')
        bot.send_sticker(call.message.chat.id,file1)
  if call.message: 
     if call.data == "45":
        text = redis.hget("logosticker","{}".format(call.from_user.id))
        link = "http://api.monsterbot.ir/pic/?text={}&color=white&y=185&font=28&fsize=60&bg=shakh19".format(text)
        urllib.urlretrieve(link, "stick.png")
        file1 = open('stick.png', 'rb')
        bot.send_sticker(call.message.chat.id,file1)
  if call.message: 
     if call.data == "46":
        text = redis.hget("logosticker","{}".format(call.from_user.id))
        link = "http://api.monsterbot.ir/pic/?text={}&color=white&y=185&font=28&fsize=60&bg=shakh17".format(text)
        urllib.urlretrieve(link, "stick.png")
        file1 = open('stick.png', 'rb')
        bot.send_sticker(call.message.chat.id,file1)
  if call.message: 
     if call.data == "47":
        text = redis.hget("logosticker","{}".format(call.from_user.id))
        link = "http://api.monsterbot.ir/pic/?text={}&color=white&y=355&font=28&fsize=104&bg=shakh15".format(text)
        urllib.urlretrieve(link, "stick.png")
        file1 = open('stick.png', 'rb')
        bot.send_sticker(call.message.chat.id,file1)
  if call.message: 
     if call.data == "48":
        text = redis.hget("logosticker","{}".format(call.from_user.id))
        link = "http://api.monsterbot.ir/pic/?color=black&font=IranNastaliq&text={}&bg=nast&fsize=190".format(text)
        urllib.urlretrieve(link, "stick.png")
        file1 = open('stick.png', 'rb')
        bot.send_sticker(call.message.chat.id,file1)
  if call.message: 
     if call.data == "49":
        text = redis.hget("logosticker","{}".format(call.from_user.id))
        link = "http://api.monsterbot.ir/pic/?text={}&color=black&font=28&fsize=100&bg=logo29&angel=0&y=300".format(text)
        urllib.urlretrieve(link, "stick.png")
        file1 = open('stick.png', 'rb')
        bot.send_sticker(call.message.chat.id,file1)
  if call.message: 
     if call.data == "50":
        text = redis.hget("logosticker","{}".format(call.from_user.id))
        link = "http://api.monsterbot.ir/pic/?text={}&color=black&font=28&fsize=100&bg=logo26&y=200".format(text)
        urllib.urlretrieve(link, "stick.png")
        file1 = open('stick.png', 'rb')
        bot.send_sticker(call.message.chat.id,file1)
  if call.message: 
     if call.data == "51":
        text = redis.hget("logosticker","{}".format(call.from_user.id))
        link = "http://api.monsterbot.ir/pic/?text={}&color=white&font=28&fsize=100&bg=logo25&angel=10&y=-240&x=160".format(text)
        urllib.urlretrieve(link, "stick.png")
        file1 = open('stick.png', 'rb')
        bot.send_sticker(call.message.chat.id,file1)
  if call.message: 
     if call.data == "52":
        text = redis.hget("logosticker","{}".format(call.from_user.id))
        link = "http://api.monsterbot.ir/pic/?text={}&color=white&font=28&fsize=50&bg=logo31&angel=-0&y=245".format(text)
        urllib.urlretrieve(link, "stick.png")
        file1 = open('stick.png', 'rb')
        bot.send_sticker(call.message.chat.id,file1)
  if call.message: 
     if call.data == "53":
        text = redis.hget("logosticker","{}".format(call.from_user.id))
        link = "http://api.monsterbot.ir/pic/?text={}&color=white&font=Cartoon&fsize=70&angel=-0&y=140&burl=http://u1.img7.ir/M7iRA.png".format(text)
        urllib.urlretrieve(link, "stick.png")
        file1 = open('stick.png', 'rb')
        bot.send_sticker(call.message.chat.id,file1)
  if call.message: 
     if call.data == "54":
        text = redis.hget("logosticker","{}".format(call.from_user.id))
        link = "http://api.monsterbot.ir/pic/?text={}&y=-30&x=20&color=Burly+Wood&font=Holool&fsize=120&bg=logo16".format(text)
        urllib.urlretrieve(link, "stick.png")
        file1 = open('stick.png', 'rb')
        bot.send_sticker(call.message.chat.id,file1)
  if call.message: 
     if call.data == "55":
        text = redis.hget("logosticker","{}".format(call.from_user.id))
        link = "http://api.monsterbot.ir/pic/?text={}&color=white&angel=0&y=275&x=0&font=ANGEL&fsize=100&bg=wallpaperhd5".format(text)
        urllib.urlretrieve(link, "stick.png")
        file1 = open('stick.png', 'rb')
        bot.send_sticker(call.message.chat.id,file1)
  if call.message: 
     if call.data == "56":
        text = redis.hget("logosticker","{}".format(call.from_user.id))
        link = "http://api.monsterbot.ir/pic/?text={}&color=black&angel=20&y=-200&x=90&font=ANGEL&fsize=100&bg=logo12".format(text)
        urllib.urlretrieve(link, "stick.png")
        file1 = open('stick.png', 'rb')
        bot.send_sticker(call.message.chat.id,file1)
  if call.message: 
     if call.data == "57":
        text = redis.hget("logosticker","{}".format(call.from_user.id))
        link = "http://api.monsterbot.ir/pic/?text={}&color=black&angel=-5&y=-10&x=0&font=ANGEL&fsize=200&bg=hardboy4".format(text)
        urllib.urlretrieve(link, "stick.png")
        file1 = open('stick.png', 'rb')
        bot.send_sticker(call.message.chat.id,file1)
  if call.message: 
     if call.data == "58":
        text = redis.hget("logosticker","{}".format(call.from_user.id))
        link = "http://api.monsterbot.ir/pic/?text={}&color=black&y=0&x=10&font=28&fsize=60&bg=logo11".format(text)
        urllib.urlretrieve(link, "stick.png")
        file1 = open('stick.png', 'rb')
        bot.send_sticker(call.message.chat.id,file1)
  if call.message: 
     if call.data == "59":
        text = redis.hget("logosticker","{}".format(call.from_user.id))
        link = "http://api.monsterbot.ir/pic/?text={}&color=white&y=355&font=28&fsize=104&bg=shakh15".format(text)
        urllib.urlretrieve(link, "stick.png")
        file1 = open('stick.png', 'rb')
        bot.send_sticker(call.message.chat.id,file1)
  if call.message: 
     if call.data == "s1":
        bot.answer_callback_query(callback_query_id=call.id, show_alert=False, text="صفحه یک")
  if call.message:
     if call.data == "s2":
        bot.answer_callback_query(callback_query_id=call.id, show_alert=False, text="صفحه دو")
  if call.message:
     if call.data == "s3":
        bot.answer_callback_query(callback_query_id=call.id, show_alert=False, text="صفحه سه")
  if call.message:
     if call.data == "s4":
        bot.answer_callback_query(callback_query_id=call.id, show_alert=False, text="صفحه چهار")
  if call.message:
     if call.data == "s5":
        bot.answer_callback_query(callback_query_id=call.id, show_alert=False, text="صفحه پنج")
  if call.message:
     if call.data == "next":
        text = redis.hget("logosticker","{}".format(call.from_user.id))
        markup = types.InlineKeyboardMarkup()
        c = types.InlineKeyboardButton("طرح گیم(انگلیسی)",callback_data='13')
        v = types.InlineKeyboardButton("طرح گیم(انگلیسی)",callback_data='14')
        markup.add(c,v)
        q = types.InlineKeyboardButton("طرح گیم(انگلیسی)",callback_data='15')
        w = types.InlineKeyboardButton("طرح گیم(انگلیسی)",callback_data='16')
        markup.add(q,w)
        e = types.InlineKeyboardButton("طرح گیم(انگلیسی)",callback_data='17')
        r = types.InlineKeyboardButton("طرح گیم(انگلیسی)",callback_data='18')
        markup.add(e,r)
        t = types.InlineKeyboardButton("طرح گیم(انگلیسی)",callback_data='19')
        y = types.InlineKeyboardButton("طرح گیم(انگلیسی)",callback_data='20')
        markup.add(t,y)
        u = types.InlineKeyboardButton("طرح گیم(انگلیسی)",callback_data='21')
        z = types.InlineKeyboardButton("طرح گیم(انگلیسی)",callback_data='22')
        markup.add(u,z)
        hg = types.InlineKeyboardButton("طرح گیم(انگلیسی)",callback_data='23')
        gh = types.InlineKeyboardButton("طرح گیم(انگلیسی)",callback_data='24')
        markup.add(hg,gh)
        ar = types.InlineKeyboardButton("◀️صفه قبل",callback_data='next0')
        kj = types.InlineKeyboardButton("2️⃣",callback_data='s2')
        jh = types.InlineKeyboardButton("صفه بعد▶️",callback_data='next2')
        markup.add(ar,kj,jh)
        bot.edit_message_text(chat_id=call.message.chat.id, message_id=call.message.message_id, text = "لطفا طرح خود را انتخاب کنید:", reply_markup=markup, parse_mode='Markdown')
  if call.message:
     if call.data == "next2":
        text = redis.hget("logosticker","{}".format(call.from_user.id))
        markup = types.InlineKeyboardMarkup()
        c = types.InlineKeyboardButton("طرح گیم(انگلیسی)",callback_data='25')
        v = types.InlineKeyboardButton("طرح گیم(انگلیسی)",callback_data='26')
        markup.add(c,v)
        q = types.InlineKeyboardButton("طرح گیم(انگلیسی)",callback_data='27')
        w = types.InlineKeyboardButton("طرح شاخ(انگلیسی)",callback_data='28')
        markup.add(q,w)
        e = types.InlineKeyboardButton("طرح شاخ(انگلیسی)",callback_data='29')
        r = types.InlineKeyboardButton("طرح شاخ(انگلیسی)",callback_data='30')
        markup.add(e,r)
        t = types.InlineKeyboardButton("طرح شاخ(انگلیسی)",callback_data='30')
        y = types.InlineKeyboardButton("طرح شاخ(انگلیسی)",callback_data='31')
        markup.add(t,y)
        u = types.InlineKeyboardButton("طرح شاخ(انگلیسی)",callback_data='32')
        z = types.InlineKeyboardButton("طرح شاخ(انگلیسی)",callback_data='33')
        markup.add(u,z)
        hg = types.InlineKeyboardButton("طرح شاخ(انگلیسی)",callback_data='34')
        gh = types.InlineKeyboardButton("طرح شاخ(انگلیسی)",callback_data='35')
        markup.add(hg,gh)
        ar = types.InlineKeyboardButton("◀️صفه قبل",callback_data='next1')
        kj = types.InlineKeyboardButton("3️⃣",callback_data='s3')
        jh = types.InlineKeyboardButton("صفه بعد▶️",callback_data='next3')
        markup.add(ar,kj,jh)
        bot.edit_message_text(chat_id=call.message.chat.id, message_id=call.message.message_id, text = "لطفا طرح خود را انتخاب کنید:", reply_markup=markup, parse_mode='Markdown')
  if call.message:
     if call.data == "next3":
        text = redis.hget("logosticker","{}".format(call.from_user.id))
        markup = types.InlineKeyboardMarkup()
        c = types.InlineKeyboardButton("طرح شاخ(انگلیسی)",callback_data='36')
        v = types.InlineKeyboardButton("طرح شاخ(انگلیسی)",callback_data='37')
        markup.add(c,v)
        q = types.InlineKeyboardButton("طرح شاخ(انگلیسی)",callback_data='38')
        w = types.InlineKeyboardButton("طرح شاخ(انگلیسی)",callback_data='39')
        markup.add(q,w)
        e = types.InlineKeyboardButton("طرح شاخ(انگلیسی)",callback_data='40')
        r = types.InlineKeyboardButton("طرح شاخ(انگلیسی)",callback_data='41')
        markup.add(e,r)
        t = types.InlineKeyboardButton("طرح شاخ(انگلیسی)",callback_data='42')
        y = types.InlineKeyboardButton("طرح شاخ(انگلیسی)",callback_data='43')
        markup.add(t,y)
        u = types.InlineKeyboardButton("طرح شاخ(انگلیسی)",callback_data='44')
        z = types.InlineKeyboardButton("طرح شاخ(انگلیسی)",callback_data='45')
        markup.add(u,z)
        hg = types.InlineKeyboardButton("طرح شاخ(انگلیسی)",callback_data='46')
        gh = types.InlineKeyboardButton("طرح شاخ(انگلیسی)",callback_data='47')
        markup.add(hg,gh)
        ar = types.InlineKeyboardButton("◀️صفه قبل",callback_data='next2')
        kj = types.InlineKeyboardButton("4️⃣",callback_data='s4')
        jh = types.InlineKeyboardButton("صفه بعد▶️",callback_data='next4')
        markup.add(ar,kj,jh)
        bot.edit_message_text(chat_id=call.message.chat.id, message_id=call.message.message_id, text = "لطفا طرح خود را انتخاب کنید:", reply_markup=markup, parse_mode='Markdown')
  if call.message:
     if call.data == "next4":
        text = redis.hget("logosticker","{}".format(call.from_user.id))
        markup = types.InlineKeyboardMarkup()
        c = types.InlineKeyboardButton("طرح لوگو (فارسی)",callback_data='48')
        v = types.InlineKeyboardButton("طرح لوگو (انگلیسی)",callback_data='49')
        markup.add(c,v)
        q = types.InlineKeyboardButton("طرح لوگو (انگلیسی)",callback_data='50')
        w = types.InlineKeyboardButton("طرح لوگو (انگلیسی)",callback_data='51')
        markup.add(q,w)
        e = types.InlineKeyboardButton("طرح لوگو (انگلیسی)",callback_data='52')
        r = types.InlineKeyboardButton("طرح لوگو (انگلیسی)",callback_data='53')
        markup.add(e,r)
        t = types.InlineKeyboardButton("طرح لوگو (فارسی)",callback_data='54')
        y = types.InlineKeyboardButton("طرح لوگو (انگلیسی)",callback_data='55')
        markup.add(t,y)
        u = types.InlineKeyboardButton("طرح لوگو (انگلیسی)",callback_data='56')
        z = types.InlineKeyboardButton("طرح لوگو (انگلیسی)",callback_data='57')
        markup.add(u,z)
        hg = types.InlineKeyboardButton("طرح لوگو (انگلیسی)",callback_data='58')
        gh = types.InlineKeyboardButton("طرح لوگو (انگلیسی)",callback_data='59')
        markup.add(hg,gh)
        ar = types.InlineKeyboardButton("◀️صفه قبل",callback_data='next3')
        kj = types.InlineKeyboardButton("5️⃣",callback_data='s5')
        markup.add(ar,kj)
        bot.edit_message_text(chat_id=call.message.chat.id, message_id=call.message.message_id, text = "لطفا طرح خود را انتخاب کنید:", reply_markup=markup, parse_mode='Markdown')
  if call.message:
     if call.data == "next0":
        text = redis.hget("logosticker","{}".format(call.from_user.id))
        markup = types.InlineKeyboardMarkup()
        c = types.InlineKeyboardButton("طرح ساده(انگلیسی)",callback_data='1')
        v = types.InlineKeyboardButton("طرح ساده(انگلیسی)",callback_data='2')
        markup.add(c,v)
        q = types.InlineKeyboardButton("طرح ساده(انگلیسی)",callback_data='3')
        w = types.InlineKeyboardButton("طرح ساده(انگلیسی)",callback_data='4')
        markup.add(q,w)
        e = types.InlineKeyboardButton("طرح ساده(انگلیسی)",callback_data='5')
        r = types.InlineKeyboardButton("طرح ساده(انگلیسی)",callback_data='6')
        markup.add(e,r)
        t = types.InlineKeyboardButton("طرح ساده(فارسی)",callback_data='7')
        y = types.InlineKeyboardButton("طرح ساده(فارسی)",callback_data='8')
        markup.add(t,y)
        u = types.InlineKeyboardButton("طرح ساده(فارسی)",callback_data='9')
        z = types.InlineKeyboardButton("طرح ساده(فارسی)",callback_data='10')
        markup.add(u,z)
        hg = types.InlineKeyboardButton("طرح ساده(فارسی)",callback_data='11')
        gh = types.InlineKeyboardButton("طرح ساده(فارسی)",callback_data='12')
        markup.add(hg,gh)
        kj = types.InlineKeyboardButton("1️⃣",callback_data='s1')
        jh = types.InlineKeyboardButton("صفه بعد▶️",callback_data='next')
        markup.add(kj,jh)
        bot.edit_message_text(chat_id=call.message.chat.id, message_id=call.message.message_id, text = "لطفا طرح خود را انتخاب کنید:", reply_markup=markup, parse_mode='Markdown')
  if call.message: 
     if call.data == "about":
        bot.edit_message_text(chat_id=call.message.chat.id, message_id=call.message.message_id, text = "سازنده:\n[sajjad_021](https://telegram.me/sajjad_021)\n\n[tgChannel](https://t.me/tgMember)\n", parse_mode='Markdown')
  if call.message: 
     if call.data == "amar":
       markup = types.InlineKeyboardMarkup()
       c = types.InlineKeyboardButton("بروز رسانی",callback_data='amar')
       markup.add(c)
       usrs = str(redis.scard('members'))
       cha = str(redis.scard('chatpy'))
       tex = 'تعداد کاربران : {}\nتعداد گروه: {}'.format(usrs,cha)
       bot.edit_message_text(chat_id=call.message.chat.id, message_id=call.message.message_id, text = tex, reply_markup=markup, parse_mode='Markdown')

bot.polling(True)