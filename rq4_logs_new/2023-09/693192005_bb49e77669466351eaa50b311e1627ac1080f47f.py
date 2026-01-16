import random

import telebot

token = "5667599798:AAHBKXN4p3RAh8ILcrtVWyX2DGhwEI6Bm9g"

bot = telebot.TeleBot(token)

RANDOM_TASKS = ["Погулять", "Поесть", "Поспать","Погладить кота"]
HELP = """
/help - вывести список доступных команд
/add - добавить задачу в справку 
/show - напечатать все добавленные задачи.
/random - добавить случайную задачу на дату Сегодня"""

tasks = {

}
def add_todo (date,task):
    if date in tasks:
        tasks[date].append(task)
    else:
        tasks[date] = []
        tasks[date] = [task]


@bot.message_handler(commands= ["help"])
def help(message):
    bot.send_message(message.chat.id, HELP)

@bot.message_handler(commands= ["add"])
def add(message):
    command = message.text.split(maxsplit=2)
    date = command[1].lower()
    task = command[2]
    add_todo(date, task)
    text = "Задача " + task + " Добавлена на дату " + date
    bot.send_message(message.chat.id, text)

@bot.message_handler(commands= ["random"])
def random_add(message):
    date = "cегодня"
    task = random.choice (RANDOM_TASKS)
    add_todo(date, task)
    text = "Задача " + task + " Добавлена на дату " + date
    bot.send_message(message.chat.id, text)

@bot.message_handler(commands= ["show", "print"])
def show(message):
    command = message.text.split(maxsplit=1)
    date = command[1].lower()
    text = ""
    if date in tasks:
        text = date.upper() + "\n"
        for task in tasks[date]:
            text = text + "[]" + task + "\n"
    else:
        text = "Задач на эту дату нет"
    bot.send_message(message.chat.id, text)
bot.polling(none_stop=True)