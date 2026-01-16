# -*- coding: utf-8 -*-

import telebot
from telebot import types
import os


bot = telebot.TeleBot("597223082:AAELhDrzTE-Mev9rf8NsTJFjsyadzZgDFRk")

user_dict = {}

class User:
    def __init__(self, name):
        self.name = name

@bot.message_handler(commands=['start'])  #команда#
def handle_start(message):
    bot.send_message(message.chat.id, "https://www.youtube.com/watch?v=olztRgAZmDA&t=6s")
    keyboard = types.InlineKeyboardMarkup(row_width=1)
    callback_button = types.InlineKeyboardButton(text="💳 Оплатить 3500 рублей",callback_data="oplata")
    callback_button1 = types.InlineKeyboardButton(text="❓Остались вопросы ", callback_data="vopros")
    keyboard.add(callback_button, callback_button1)
    bot.send_message(message.chat.id, " *🔥 Litvin Stavit*  \nМесячная подписка \n*После оплаты у тебя будет:*\n \n *1⃣ Личный кабинет* в телеграм-боте с обучением по ставкам. (Как ставить? Где ставить? Доп. техники. И тд)\n*2⃣ 130-150 прогнозов* в месяц со средней доходностью 280% в месяц. 4-6 ставок в день с проходимостью 85%\n3⃣ *Полное сопровождение* по всем ставкам + помощь по любым вопросам в течении всего месяца\n4⃣ *Дополнительный бонус* от Litvin Stavit после оплаты\n\n✅ В среднем вложенные деньги отбиваются за 3 дня\n\n💳 Стоимость: 3500 рублей\n⬇️*Если готов начать, Жми*",parse_mode="Markdown",reply_markup=keyboard)
    user_markup = telebot.types.ReplyKeyboardMarkup(True) #клавиатура#
    user_markup.row('💳 Оплатить 3500 рублей')
    user_markup.row('❓Остались вопросы')
    directory = 'C:/Users/user/PycharmProjects/Telegram/files/video'
    all_files_in_directory = os.listdir(directory)
    print(all_files_in_directory)
    for file in all_files_in_directory:
        img = open(directory + '/' + file, 'rb')
        bot.send_video_note(message.chat.id, img)
        img.close()


@bot.message_handler(func=lambda m: m.text in ('stop', '/stop', '💳 Оплатить 3500 рублей'))
def send_welcome( message):
    msg = bot.send_message(message.chat.id, "📧 Введите свой e-mail")
    bot.register_next_step_handler(msg, process_name_step)


def process_name_step(message):
    try:
        chat_id = message.chat.id
        name = message.text
        user = User(name)
        user_dict[chat_id] = user
        hide_markup = telebot.types.ReplyKeyboardMarkup()
        url_button5 = types.InlineKeyboardButton(text='💳 Оплатить 3500 рублей', url='http://www.free-kassa.ru/merchant/cash.php?m=99072&oa=3500&o=9466&s=791b3699e6cf8d40753f981b4b2008bd&lang=ru&i=&em='+user.name+'.com&us_id=9489')
        keyboard = types.InlineKeyboardMarkup(row_width=1)
        keyboard.add(url_button5)

        bot.send_message(chat_id, '✅ Спасибо,'+ str(message.from_user.first_name) +' \nE-mail: '  + user.name + "\n📲 Ваш личный кабинет готов. После оплаты, вы сразу получите доступ\n\nВаша ссылка на оплату генерируется...", parse_mode="Markdown")
        bot.send_message(message.chat.id,'Нажмите на кнопку ниже - вы перейдете на страницу где сможете выбрать способ оплаты',reply_markup=keyboard)


    except Exception as e:
        bot.reply_to(message, 'oooops')


@bot.message_handler(func=lambda m: m.text in ('help', '/help', '❓Остались вопросы'))
def handle_help(message):
    hide_markup = telebot.types.ReplyKeyboardMarkup()
    keyboard = types.InlineKeyboardMarkup(row_width=1)
    callback_button3 = types.InlineKeyboardButton(text="📊 Статистика прогнозов?", callback_data="stat")
    url_button4 = types.InlineKeyboardButton(text="📲 Соц-сети проекта?", callback_data="soc")
    url_button5 = types.InlineKeyboardButton(text='⚽ Бесплатный канал?', url='https://t.me/mmbet24')
    url_button6 = types.InlineKeyboardButton(text='💻 Сайт проекта', url='https://litvinstavit.ru/')
    url_button7 = types.InlineKeyboardButton(text="📃 Пользовательское соглашение?", callback_data="sogl")
    url_button8 = types.InlineKeyboardButton(text="💳 Проблема с оплатой ?", callback_data="opl")
    url_button9 = types.InlineKeyboardButton(text="👋 Задать свой вопрос", callback_data="vopross")
    keyboard.add(callback_button3, url_button4, url_button5, url_button6, url_button7, url_button8, url_button9)
    bot.send_message(message.from_user.id, "⬇️ Нажмите на интересующий вопрос, и вы тут же получите ответ",reply_markup=keyboard)


@bot.callback_query_handler(func=lambda message: True)
def callback_inline(message):
    if message:
        if message.data == "vopros":
            keyboard = types.InlineKeyboardMarkup(row_width=1)
            callback_button3 = types.InlineKeyboardButton(text="📊 Статистика прогнозов?", callback_data="stat")
            url_button4 = types.InlineKeyboardButton(text="📲 Соц-сети проекта?", callback_data="soc")
            url_button5 = types.InlineKeyboardButton(text='⚽ Бесплатный канал?', url='https://t.me/mmbet24')
            url_button6 = types.InlineKeyboardButton(text='💻 Сайт проекта', url='https://litvinstavit.ru/')
            url_button7 = types.InlineKeyboardButton(text="📃 Пользовательское соглашение?", callback_data="sogl")
            url_button8 = types.InlineKeyboardButton(text="💳 Проблема с оплатой ?", callback_data="opl")
            url_button9 = types.InlineKeyboardButton(text="👋 Задать свой вопрос", callback_data="vopross")
            keyboard.add( callback_button3, url_button4, url_button5, url_button6, url_button7, url_button8, url_button9)
            bot.send_message(message.from_user.id,"⬇️ Нажмите на интересующий вопрос, и вы тут же получите ответ", reply_markup=keyboard)
        elif message.message:
            if message.data == "stat":
                keyboard = types.InlineKeyboardMarkup(row_width=1)
                callback_button5 = types.InlineKeyboardButton(text="Ноябрь", callback_data="noyab")
                callback_button6 = types.InlineKeyboardButton(text="Октябрь", callback_data="okt")
                callback_button7 = types.InlineKeyboardButton(text="Сентябрь", callback_data="sent")
                callback_button8 = types.InlineKeyboardButton(text="Август", callback_data="avg")
                callback_button9 = types.InlineKeyboardButton(text="Июль", callback_data="iul")
                keyboard.add(callback_button5,callback_button6,callback_button7,callback_button8,callback_button9)
                bot.send_message(message.from_user.id, "🌐 Выберите месяц",reply_markup=keyboard)
            elif message.message:
                #📊 Статистика прогнозов?
                if message.data == "noyab":
                    bot.send_message(message.from_user.id, "❗️    11")
                elif message.message:
                    if message.data == "okt":
                        bot.send_message(message.from_user.id, "*Итоги 1-10 октября*\n🏁Ставок 70\n✅Выигрыш 42(+)\n♻️Возврат 5 (=)\n❌Проигрыш 23 (-)\n📊+ 86% (если ставить по 10% от изначального банка на каждый прогноз)\n🏦Баланс(с расчётом 1000 рублей на каждую ставку) + 8600 рублей к своему банку за 10 дней\n\n*Итоги 11-20 октября*\n🏁Ставок 59\n✅Выигрыш 44(+)\n♻️Возврат 3(=)\n❌Проигрыш 12 (-)\n📊+ 207% (если ставить по 10% от изначального банка на каждый прогноз)\n🏦Баланс(с расчётом 1000 рублей на каждую ставку) + 20700 рублей к своему банку за 10 дней\n\n❗️До 20 октбяря, мы давали все прогнозы на наш беслатный канал [MMBET](https://t.me/mmbet24), чтобы все люди могли убедится в нашей экспертности. Поэтому всю достовернность нижеперечисленных данных вы можете сверить в нашем канале",parse_mode="Markdown")
                    elif message.message:
                        if message.data == "sent":
                            bot.send_message(message.from_user.id, "*Итоги 1-10 сентября* \n🏁Ставок 45\n✅Выигрыш 26(+)\n♻️Возврат 5(=)\n❌Проигрыш 14(-)\n📊+ 57% (если ставить по 10% от изначального банка на каждый прогноз)\n🏦Баланс(с расчётом 1000 рублей на каждую ставку) + 5700 рублей к своему банку за 20 дней\n\n*Итоги 11-20 сентября *\n🏁Ставок 68\n✅Выигрыш 40(+)\n♻️Возврат 4(=)\n❌Проигрыш 24(-)\n📊+ 62% (если ставить по 10% от изначального банка на каждый прогноз)\n🏦Баланс(с расчётом 1000 рублей на каждую ставку) + 6200 рублей к своему банку за 10 дней\n\n*Итоги 21-30 сентября *\n🏁Ставок 58\n✅Выигрыш 39(+)\n♻️Возврат 4(=)\n❌Проигрыш 15(-)\n📊+ 145% (если ставить по 10% от изначального банка на каждый прогноз)\n🏦Баланс(с расчётом 1000 рублей на каждую ставку) + 14500 рублей к своему банку за 10 дней\n\n❗️До 20 октбяря, мы давали все прогнозы на наш беслатный канал [MMBET](https://t.me/mmbet24), чтобы все люди могли убедится в нашей экспертности. Поэтому всю достовернность нижеперечисленных данных вы можете сверить в нашем канале",parse_mode="Markdown")
                        elif message.message:
                            if message.data == "avg":
                                bot.send_message(message.from_user.id, "*Итоги 1-10 августа*\n🏁Ставок 55\n✅Выигрыш 33(+)\n♻️Возврат 6(=)\n❌Проигрыш 16 (-)\n📊+ 92% (если ставить по 10% от изначального банка на каждый прогноз)\n🏦Баланс(с расчётом 1000 рублей на каждую ставку) + 9200 рублей к своему банку за 10 дней\n\n*Итоги 11-20 августа*\n🏁Ставок 60\n✅Выигрыш 35(+)\n♻️Возврат 5(=)\n❌Проигрыш 20 (-)\n📊+ 61% (если ставить по 10% от изначального банка на каждый прогноз)\n🏦Баланс(с расчётом 1000 рублей на каждую ставку) + 6100 рублей к своему банку за 10 дней\n\n*Итоги 21-31 августа* \n🏁Ставок 74\n✅Выигрыш 46(+)\n♻️Возврат 3(=)\n❌Проигрыш 25 (-)\n📊+ 93% (если ставить по 10% от изначального банка на каждый прогноз)\n🏦Баланс(с расчётом 1000 рублей на каждую ставку) + 9300 рублей к своему банку за 10 дней\n\n❗️До 20 октбяря, мы давали все прогнозы на наш беслатный канал [MMBET](https://t.me/mmbet24), чтобы все люди могли убедится в нашей экспертности. Поэтому всю достовернность нижеперечисленных данных вы можете сверить в нашем канале",parse_mode="Markdown")
                            elif message.message:
                                if message.data == "iul":
                                    bot.send_message(message.from_user.id, "*Итоги 1-10 июля*\n🏁Ставок 90\n✅Выигрыш 64(+)\n♻️Возврат 7(=)\n❌Проигрыш 19 (-)\n📊+ 290% (если ставить по 10% от изначального банка на каждый прогноз)\n🏦Баланс(с расчётом 1000 рублей на каждую ставку) + 29000 рублей к своему банку за 10 дней\n\n*Итоги 11-20 июля*\n🏁Ставок 96\n✅Выигрыш 58(+)\n♻️Возврат 7(=)\n❌Проигрыш 31(-)\n📊+ 127% (если ставить по 10% от изначального банка на каждый прогноз)\n🏦Баланс(с расчётом 1000 рублей на каждую ставку) + 12700 рублей к своему банку за 10 дней\n\n*Итоги 21-31 июля*\n🏁Ставок 48\n✅Выигрыш 29(+)\n♻️Возврат 3(=)\n❌Проигрыш 16(-)\n📊+ 55% (если ставить по 10% от изначального банка на каждый прогноз)\n🏦Баланс(с расчётом 1000 рублей на каждую ставку) + 5500 рублей к своему банку за 10 дней\n\n*Итоги ИЮЛЬ*\n🏁Ставок 234\n✅Выигрыш 151(+)\n♻️Возврат 17(=)\n❌Проигрыш 66(-)\n📊+ 472% (если ставить по 10% от изначального банка на каждый прогноз)\n🏦Баланс(с расчётом 1000 рублей на каждую ставку) + 47200 рублей к своему банку за 30 дней\n\n❗️До 20 октбяря, мы давали все прогнозы на наш беслатный канал [MMBET](https://t.me/mmbet24), чтобы все люди могли убедится в нашей экспертности. Поэтому всю достовернность нижеперечисленных данных вы можете сверить в нашем канале",parse_mode="Markdown")
                                elif message.message:
                                    # Соц-сети проекта?
                                    if message.data == "soc":
                                        bot.send_message(message.from_user.id, "📱 INST [https://instagram.com/litvin_stavit](https://instagram.com/litvin_stavit)\n📹 YouTube [https://www.youtube.com/channel/UCJNGDA1iq4Gqqqhl7CVDUBA?view_as=subscriber](https://www.youtube.com/channel/UCJNGDA1iq4Gqqqhl7CVDUBA?view_as=subscriber)\n💻 VK [https://vk.com/litvin_stavit](https://vk.com/litvin_stavit)",parse_mode="Markdown")
                                    elif message.message:
                                        #Пользовательское соглашение?
                                        if message.data == "sogl":
                                            bot.send_message(message.from_user.id, "Прочитав наш документ вы сможете ознакомиться с пользовательским соглашением\n\n[https://litvinstavit.ru/files/Litvin%20Stavit%20Соглашение.docx](https://litvinstavit.ru/files/Litvin%20Stavit%20Соглашение.docx)",parse_mode="Markdown")
                                        #Проблема с оплатой
                                        elif message.message:
                                            if message.data == "opl":
                                                bot.send_message(message.from_user.id,"Если у вас возникли проблемы с оплатой, напишите нашему саппорту\n \n[@Litvin_Supp](@Litvin_Supp)",parse_mode="Markdown")
                                            elif message.message:
                                                #Задать свой вопрос
                                                if message.data == "vopross":
                                                    bot.send_message(message.from_user.id,"Если у вас возникли проблемы с оплатой, напишите нашему саппорту\n\n[@Litvin_Supp](@Litvin_Supp)",parse_mode="Markdown")
                                                elif message.message:
                                                    if message.data == "oplata":
                                                        msg = bot.send_message(message.from_user.id,"📧 Введите свой e-mail")
                                                        bot.register_next_step_handler(msg, process_name_step)


if __name__ == '__main__':
    bot.polling(none_stop=True)