import telegram.ext
import telebot
from telegram.ext import Updater, CommandHandler, MessageHandler, Filters, ConversationHandler

Token = "5723169972:AAG3Nq6JTSGuSTvtfgtY4UmUduktOCJJvVM"

bot = telebot.TeleBot("5723169972:AAG3Nq6JTSGuSTvtfgtY4UmUduktOCJJvVM", parse_mode=None)

updater = telegram.ext.Updater("5723169972:AAG3Nq6JTSGuSTvtfgtY4UmUduktOCJJvVM", use_context=True)

dispatcher = updater.dispatcher

def start(update, context):
    update.message.reply_text("Hello. I'm your WeCare Chatbot. My purpose is to smoothly assist you in navigating through our entire system. \n\n/about About Our System \n/vitals View Vital Sign History \n/intake View Medicine Intake History \n/web Website Link \n/help View Help \n\nKindly type or click the commands highlighted to redirect you to your choice.")

def about(update, context):
    update.message.reply_text("Our automated medicine dispenser aims to help people with chronic diseases in monitoring and making sure that they will not miss any of their medications. ")

def vitals(update, context):
    update.message.reply_text("You have chosen to view your vital sign history. Click the link to redirect you to our website.\n\nhttps://docs.google.com/spreadsheets/d/1CLqKWCDC7Bria-MwvTOzH4XUh4gp5D9lDzn0052pGsk/edit#gid=0")

def intake(update, context):
    update.message.reply_text(" Hatdog : www.youtube.com ")

def web(update, context):
    update.message.reply_text("Here is the link to our website.\n\n")

def help(update, context):
    update.message.reply_text("In which area do you need our help?\n\n[1]/dispenser Medicine Dispenser Guide\n[2]/website Website Guide\n\nPlease click on any of the commands.")

def dispenser(update, context):
    update.message.reply_text("Here is the guide for our Medicine Dispenser.\n\nFirst and foremost, our dispenser contains three (3) buttons. These buttons are used to configure the alarm for the medicine intake, each with different functionalities.")

def website(update, context):
    update.message.reply_text("Our website contains _ main tabs.")





def photo(update, context):
    update.message.reply_text("I'm sorry, but I cannot understand photos. Please enter a command provided from the list mentioned upon launching this bot or click /start")

photo_handler = telegram.ext.MessageHandler(telegram.ext.Filters.photo, photo)

def sticker(update, context):
    update.message.reply_text("I'm sorry, but stickers are not recognized as commands. Please enter a command provided from the list mentioned upon launching this bot or click /start")

sticker_handler = telegram.ext.MessageHandler(telegram.ext.Filters.sticker, sticker)

def audio(update, context):
    update.message.reply_text("I'm sorry, but I cannot decipher audios. Please enter a command provided from the list mentioned upon launching this bot or click /start")

audio_handler = telegram.ext.MessageHandler(telegram.ext.Filters.audio, audio)

def document(update, context):
    update.message.reply_text("I'm sorry, but I cannot read documents or files. Please enter a command provided from the list mentioned upon launching this bot or click /start")

document_handler = telegram.ext.MessageHandler(telegram.ext.Filters.document, document)

def video(update, context):
    update.message.reply_text("I'm sorry, but I cannot analyze videos. Please enter a command provided from the list mentioned upon launching this bot or click /start")

video_handler = telegram.ext.MessageHandler(telegram.ext.Filters.video, video)

dispatcher.add_handler(video_handler)

dispatcher.add_handler(document_handler)

dispatcher.add_handler(audio_handler)

dispatcher.add_handler(sticker_handler)

dispatcher.add_handler(photo_handler)


dispatcher.add_handler(telegram.ext.CommandHandler('start', start))

dispatcher.add_handler(telegram.ext.CommandHandler('about', about))

dispatcher.add_handler(telegram.ext.CommandHandler('vitals', vitals))

dispatcher.add_handler(telegram.ext.CommandHandler('intake', intake))

dispatcher.add_handler(telegram.ext.CommandHandler('web', web))

dispatcher.add_handler(telegram.ext.CommandHandler('help', help))

dispatcher.add_handler(telegram.ext.CommandHandler('dispenser', dispenser))

dispatcher.add_handler(telegram.ext.CommandHandler('website', website))

updater.start_polling()
updater.idle() 