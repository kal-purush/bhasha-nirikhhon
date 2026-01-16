#!/usr/local/bin/python3.6
# -*- coding: utf-8 -*-
import config
import telebot
import cherrypy
import send_msg
import shelve
from telebot import types
#import logging

#logger = telebot.logger
#telebot.logger.setLevel(logging.INFO)

AllKeyboardButtonOne = types.KeyboardButton(text='Сoздaть обрaщeниe')
AllKeyboardButtonTwo = types.KeyboardButton(text='Пoсмoтреть другиe уcлуги')
AllKeyboard = types.ReplyKeyboardMarkup(resize_keyboard=True,one_time_keyboard=False)
AllKeyboard.row(AllKeyboardButtonOne)
AllKeyboard.row(AllKeyboardButtonTwo) # Включены латинские буквы в целях исключения ручного ввода

RequestKeyboard = types.ReplyKeyboardMarkup(resize_keyboard=True,one_time_keyboard=False)
RequestKeyboard.row('Oтпpaвить oбрaщeние') # Включены латинские буквы в целях исключения ручного ввода

CellPhoneSendKeyboard = types.ReplyKeyboardMarkup(resize_keyboard=True,one_time_keyboard=False)
button_phone = types.KeyboardButton(text="Отправить номер телефона", request_contact=True)
CellPhoneSendKeyboard.add(button_phone)

CellPhoneRetryKeyboard = types.ReplyKeyboardMarkup(resize_keyboard=True,one_time_keyboard=False)
button_phone = types.KeyboardButton(text="Отправить номер телефона", request_contact=True)
CellPhoneRetryKeyboard.add(button_phone)
CellPhoneRetryKeyboard.row('Пeрeйти в Глaвнoе мeню') # Включены латинские буквы в целях исключения ручного ввода

HideKeyboard = types.ReplyKeyboardRemove()

URLKeyboard = types.InlineKeyboardMarkup()
TelecomURLBotton = types.InlineKeyboardButton(text="Перейти в Astana EXPO-2017", url="https://expo2017astana.com/")
URLKeyboard.add(TelecomURLBotton)

bot = telebot.TeleBot(config.token)

strmessage = '0'
db = shelve.open("ClientRequestHistory", writeback=True)

# WebhookServer, process webhook calls
class WebhookServer(object):
    @cherrypy.expose
    def index(self):
        if 'content-length' in cherrypy.request.headers and \
           'content-type' in cherrypy.request.headers and \
           cherrypy.request.headers['content-type'] == 'application/json':
            length = int(cherrypy.request.headers['content-length'])
            json_string = cherrypy.request.body.read(length).decode("utf-8")
            update = telebot.types.Update.de_json(json_string)
            bot.process_new_updates([update])
            log('vse ok')
            return ''
        else:
            raise cherrypy.HTTPError(403)

@bot.message_handler(content_types=["contact"])
def test(message):
    print(message.contact)

    if db.get(str(message.chat.id) + '_1') == 'start':
        db[str(message.chat.id) + '_2'] = message.contact.phone_number
        bot.send_message(message.chat.id, config.RequestMessageLineAll, reply_markup=HideKeyboard)

@bot.message_handler(commands=['start'])
def command_start(m):
    print('Block: /start')
    bot.send_message(message.chat.id, config.WelcomeMessageLineAll, reply_markup=AllKeyboard)

@bot.message_handler(content_types=["text"])
def test(message):

    if message.text == '/start':
        print('Block: /start')
        bot.send_message(message.chat.id, config.WelcomeMessageLineAll, reply_markup=AllKeyboard)

    elif message.text == 'Сoздaть обрaщeниe':
        print('Block: Сoздaть обрaщeниe')
        bot.send_message(message.chat.id, config.SendPhoneMessageLine, reply_markup=CellPhoneSendKeyboard)
        #bot.send_message(message.chat.id, config.RequestMessageLineAll,reply_markup=HideKeyboard)
        db[str(message.chat.id)+'_1'] = 'start'
        db[str(message.chat.id)+'_2'] = ''

    elif message.text == 'Пoсмoтреть другиe уcлуги':
        print('Block: Пoсмoтреть другиe уcлуги')
        bot.send_message(message.chat.id, "Для ознакомления с другими услугами нажмите на кнопку «Перейти в Astana EXPO-2017»", reply_markup=URLKeyboard)

    elif message.text == 'Пeрeйти в Глaвнoе мeню':
        print('Block: Пeрeйти в Глaвнoе мeню')
        bot.send_message(message.chat.id, config.WelcomeMessageLineAll, reply_markup=AllKeyboard)

    else:
        if db.get(str(message.chat.id)+'_1') == 'start' and db.get(str(message.chat.id)+'_2') == '':
            print('Block: Создание обращения: Не передали номер')
            bot.send_message(message.chat.id, config.SendPhoneRertyMessageLineAll, reply_markup=CellPhoneRetryKeyboard)

        elif db.get(str(message.chat.id)+'_1') == 'start' and db.get(str(message.chat.id)+'_2') != '':
            print('Block: else check start')

            # номер телефона - db.get(str(message.chat.id)+'_2')
            # текст обращения - message.text
            send_msg.send(message.text, db.get(str(message.chat.id)+'_2'), str(message.chat.id), 'Telegram')

            db[str(message.chat.id)+'_1'] = 'end'
            db[str(message.chat.id)+'_2'] = ''
            bot.send_message(message.chat.id, config.AddRequestMessageLineAll, reply_markup=AllKeyboard)
        else:
            print('Block: аналог \start')
            bot.send_message(message.chat.id, config.WelcomeMessageLineAll, reply_markup=AllKeyboard)


# Remove webhook, it fails sometimes the set if there is a previous webhook
bot.remove_webhook()

# Set webhook
bot.set_webhook(url=config.WEBHOOK_URL_BASE+config.WEBHOOK_URL_PATH,
                certificate=open(config.WEBHOOK_SSL_CERT, 'r'))

# Start cherrypy server
cherrypy.config.update({
    'server.socket_host': config.WEBHOOK_LISTEN,
    'server.socket_port': config.WEBHOOK_PORT,
    'server.ssl_module': 'builtin',
    'server.ssl_certificate': config.WEBHOOK_SSL_CERT,
    'server.ssl_private_key': config.WEBHOOK_SSL_PRIV
})

cherrypy.quickstart(WebhookServer(), config.WEBHOOK_URL_PATH, {'/': {}})