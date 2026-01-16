# ваш код телеграм-бота
import os
import dotenv
import telebot
from telebot import types

from apps.authorization.models import User

dotenv.load_dotenv()

# Ваш токен бота
bot_token = os.getenv('BOT_TOKEN')
bot = telebot.TeleBot(bot_token)

# Словарь для хранения данных пользователя в контексте
user_data = {}

@bot.message_handler(commands=['start', 'help'])
def send_welcome(message):
    bot.reply_to(message, "Привет! Я твой тестовый бот.")

@bot.message_handler(commands=['users'])
def get_users(message):
    users = User.objects.all()
    for user in users:
        bot.send_message(message.chat.id, f'ID: {user.id}, Имя: {user.username}, Email: {user.email}, Телефон: {user.phone_number}, Активен: {user.is_active}, Сотрудник: {user.is_staff}')
    bot.send_message(message.chat.id, f'Количество пользователей: {len(users)}')
    markup = types.ReplyKeyboardMarkup(resize_keyboard=True)
    item1 = types.KeyboardButton('Добавить пользователя')
    markup.add(item1)
    bot.send_message(message.chat.id, 'Выберите действие:', reply_markup=markup)
    bot.register_next_step_handler(message, add_user)

@bot.message_handler(commands=['add_user'])
def add_user(message):
    bot.send_message(message.chat.id, 'Введите email пользователя:')
    bot.register_next_step_handler(message, add_email)

def add_email(message):
    email = message.text
    # Сохраняем email в контексте
    user_data['email'] = email
    bot.send_message(message.chat.id, 'Введите имя пользователя:')
    bot.register_next_step_handler(message, add_username)

def add_username(message):
    username = message.text
    # Получаем email из контекста
    email = user_data.get('email', None)
    
    if email:
        # Создайте экземпляр пользователя и сохраните его
        user = User.objects.create(email=email, username=username, telegram_id=message.chat.id)
        bot.send_message(message.chat.id, f'Пользователь {user.username} успешно добавлен.')
        bot.send_message(message.chat.id, 'Для продолжения выберите действие:', reply_markup=types.ReplyKeyboardRemove())
        bot.register_next_step_handler(message, get_users)
    else:
        bot.send_message(message.chat.id, 'Что-то пошло не так. Пожалуйста, попробуйте снова.')

def main():
    print()
    bot.polling(none_stop=True)