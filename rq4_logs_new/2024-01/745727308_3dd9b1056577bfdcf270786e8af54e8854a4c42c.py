# https://t.me/questRumaBot
# Папка с картинками: https://drive.google.com/drive/folders/1GcHE2jHhqKJQhoQPoh-cQvP9VEaKJ14m?usp=sharing

# Выдайте пожалуйста доступ к серверу

import telebot
import json

API_TOKEN = '6663429185:AAF8LUhVivGVHpCdYoJE4t8tb8n7DykdxO8'

bot = telebot.TeleBot(API_TOKEN)

user_data = {}

with open('game_data.json', 'r', encoding='utf8') as file:
    game_data = json.load(file)


def save_user_data():
    with open('user_data.json', 'w', encoding='utf8') as file_for_json:
        json.dump(user_data, file_for_json, ensure_ascii=False)


@bot.message_handler(commands=['start'])
def start(message):
    bot.send_message(message.chat.id, "Привет! Я текстовый бот-квест.")
    bot.send_message(message.chat.id,
                     "Для начала прохождения квеста используй команду /quest. Для ознакомления с командами бота: /help")
    user_id = message.from_user.id
    user_data[user_id] = {}
    user_data[user_id]['name'] = message.from_user.first_name

    save_user_data()


@bot.message_handler(commands=['help'])
def send_welcome(message):
    bot.reply_to(message, """\
Команды бота:
/start - Начало работы
/quest - Начать квест
\
""")


@bot.message_handler(commands=['quest'])
def quest(message):
    try:
        user_id = message.from_user.id
        if user_id not in user_data:
            user_data[user_id] = {}
        user_data[user_id]['name'] = message.from_user.first_name

        location0_data = game_data['location0']
        description = location0_data['description']
        options = location0_data['options']

        with open('game_media/Локация 0.jpg', 'rb') as photo:
            bot.send_photo(message.chat.id, photo)
        response = f"{description}"

        keyboard = telebot.types.ReplyKeyboardMarkup(resize_keyboard=True)
        for option in options.keys():
            keyboard.add(option)

        bot.send_message(message.chat.id, response, reply_markup=keyboard)

        save_user_data()
    except:
        bot.reply_to(message, f"Произошла ошибка! Попробуйте ещё раз или введите /start")


@bot.message_handler(func=lambda message: True)
def handle_options(message):
    try:
        user_id = message.from_user.id
        if user_id not in user_data:
            user_data[user_id] = {}
        user_data[user_id]['name'] = message.from_user.first_name

        # Проверяем, есть ли выбранный опцию в текущей локации
        current_location_data = game_data[user_data[user_id].get('location', 'location0')]
        options = current_location_data['options']
        if message.text in options:
            # Переходим на новую локацию
            user_data[user_id]['location'] = options[message.text]
            location_data = game_data[options[message.text]]
            description = location_data['description']
            options = location_data['options']

            # Получаем путь к картинке из json файла
            image_path = location_data['image']

            # Отправляем картинку пользователю
            with open(image_path, 'rb') as photo:
                bot.send_photo(message.chat.id, photo)

            response = f"{description}"

            keyboard = telebot.types.ReplyKeyboardMarkup(resize_keyboard=True)
            for option in options.keys():
                keyboard.add(option)

            bot.send_message(message.chat.id, response, reply_markup=keyboard)

            save_user_data()

        if not options:  # Если опций не осталось
            bot.send_message(message.chat.id, "Игра окончена. Спасибо за игру!",
                             reply_markup=telebot.types.ReplyKeyboardRemove())
            # Создаем клавиатуру
            keyboard = telebot.types.ReplyKeyboardMarkup(row_width=1, resize_keyboard=True)
            start_button = telebot.types.KeyboardButton('/start')
            keyboard.add(start_button)
            bot.send_message(message.chat.id, "Начать заново?", reply_markup=keyboard)

            save_user_data()
    except:
        bot.reply_to(message, f"Произошла ошибка! Попробуйте ещё раз или введите /start")


bot.polling()