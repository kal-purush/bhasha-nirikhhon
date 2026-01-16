import telebot
import config
from keyboards import markup_main, category, list_of_tovars

bot = telebot.TeleBot(config.token)

menu = [['Эспрессо', '60 мл', '70'],
        ['Допио (двойной эспрессо)', '100 мл', '110'],
        ['Американо', '250 мл', '80']]


@bot.message_handler(commands=['start'])
def start(message):
    bot.send_message(message.chat.id,
                     'Привет!',
                     reply_markup=markup_main())


@bot.callback_query_handler(lambda call: call.data == 'main_menu')
def main_menu(call):
    bot.edit_message_text(chat_id=call.message.chat.id,
                          message_id=call.message.message_id,
                          text='Выберите категорию',
                          reply_markup=category())


@bot.callback_query_handler(lambda call: call.data == 'drinks')
def drinks(call):
    i = 0

    bot.edit_message_text(chat_id=call.message.chat.id,
                          message_id=call.message.message_id,
                          text=f'{" ".join(menu[0])}',
                          reply_markup=list_of_tovars(menu, i))


@bot.callback_query_handler(lambda call: 'switch' in call.data)
def switch_(call):
    i = int(call.data.split('_')[1])

    bot.edit_message_text(chat_id=call.message.chat.id,
                          message_id=call.message.message_id,
                          text=f'{" ".join(menu[i])}',
                          reply_markup=list_of_tovars(menu, i))


@bot.callback_query_handler(lambda call: call.data == 'exit')
def exit_(call):
    bot.edit_message_text(chat_id=call.message.chat.id,
                          message_id=call.message.message_id,
                          text='Вы в Главном Меню!',
                          reply_markup=markup_main())


bot.polling()