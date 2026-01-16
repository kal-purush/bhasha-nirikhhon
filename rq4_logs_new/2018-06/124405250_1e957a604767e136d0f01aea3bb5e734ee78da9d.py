import logging
from telegram.ext import Updater, CommandHandler, MessageHandler, Filters
from telegram import Chat, Contact

def start(bot, update):
    bot.send_message(chat_id=update.message.chat_id, text="Bem vindo ao " + bot.first_name + "!")

def echo(bot, update):
    user_name = update.message.chat['first_name'] + " " + update.message.chat['last_name']
    user_msg = update.message.text
    bot.send_message(chat_id=update.message.chat_id, text="Olá " + user_name + ", você enviou a mensagem: " + user_msg)

def unknown(bot, update):
    bot.send_message(chat_id=update.message.chat_id, text="Desculpe, não entendi o comando:" + update.message.text)

def finish(bot, update):
    bot.send_message(chat_id=update.message.chat_id, text="Atividades encerradas! Até mais...")
    updater.stop()                     

updater = Updater(token='595074795:AAHVCXh3NJ1_1Qmw59biLPDIbub8JPdKp9k')

logging.basicConfig(format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
                    level=logging.INFO)

dispatcher = updater.dispatcher

start_handler= CommandHandler('start', start)

dispatcher.add_handler(start_handler)

echo_handler = MessageHandler(Filters.text, echo)

dispatcher.add_handler(echo_handler)

unknown_handler = MessageHandler(Filters.text, unknown)

dispatcher.add_handler(unknown_handler)

finish_handler = CommandHandler('finish', finish)

dispatcher.add_handler(finish_handler)

updater.start_polling()
