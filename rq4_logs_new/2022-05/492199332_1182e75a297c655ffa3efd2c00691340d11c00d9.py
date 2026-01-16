import telebot
bot = telebot.Telebot("5242357126:AAG5Yjs-ETTpaBIUXLaSwcX1_MtDvz5dkOI")

@bot.message_hendler(commands = ["start"])

def start(message):
    bot.send_message(message.chat.id , "<b>Добрый день</b>" , parse_mode = "html")


bot.polling(none_stop = True)