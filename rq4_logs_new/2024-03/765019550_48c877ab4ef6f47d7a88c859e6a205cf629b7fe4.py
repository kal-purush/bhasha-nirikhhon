from urllib import request
from telebot import types

from django.core.files import File
from django.core.files.temp import NamedTemporaryFile
from django.core.validators import validate_email

from apps.customers.models import Client
from apps.authorization.models import User

from .settings import bot, bot_token

user_data = {}

@bot.message_handler(commands=['register'])
def register(message):
    if User.objects.filter(telegram_id=message.chat.id).exists():
        bot.send_message(message.chat.id, 'Вы уже Авторизованы!')
        return
    bot.send_message(message.chat.id, 'Введите email пользователя:')
    bot.register_next_step_handler(message, add_email)

def add_email(message):
    email = message.text
    # Сохраняем email в контексте
    try: 
        validate_email(email)
    except:
        bot.send_message(message.chat.id, 'Некорректный email. Пожалуйста, введите корректный email.')
        bot.register_next_step_handler(message, add_email)
        return
    if User.objects.filter(email=email).exists():
        bot.send_message(message.chat.id, 'Пользователь с таким email уже существует. Пожалуйста, введите другой email.')
        bot.register_next_step_handler(message, add_email)
        return
    user_data['email'] = email
    bot.send_message(message.chat.id, 'Введите имя пользователя:')
    bot.register_next_step_handler(message, add_username)

def add_username(message):
    username = message.text
    # Получаем email из контекста
    email = user_data.get('email', None)
    
    if email:
        # Создайте экземпляр пользователя и сохраните его
        user = User.objects.create(
            email=email, username=username, telegram_id=message.chat.id
            )
        bot.send_message(message.chat.id, f'Пользователь {user.username} успешно добавлен.')
        
        markup = types.ReplyKeyboardMarkup(resize_keyboard=True)
        item1 = types.KeyboardButton('Подтвердить номер', request_contact=True)
        markup.add(item1)
        bot.send_message(message.chat.id, 
                         'Подтвердитеe телефон для регистрации. Нажмите на кнопку "Подтвердить номер".', 
                         reply_markup=markup)
    else:
        bot.send_message(message.chat.id, 'Что-то пошло не так. Пожалуйста, попробуйте снова.')


@bot.message_handler(content_types=['contact'])
def contact(message):
    if not User.objects.filter(telegram_id=message.chat.id).exists():
        bot.send_message(message.chat.id, 'Вы не авторизованы!')
        return

    user = User.objects.get(telegram_id=message.chat.id)
    user.phone_number = message.contact.phone_number
    user.save()

    # Создание экземпляра Client и сохранение фотографии из профиля Telegram
    client = Client(user=user)

    # Получение информации о файле фотографии
    file_id = bot.get_user_profile_photos(user.telegram_id, limit=1).photos[0][-1].file_id
    file_info = bot.get_file(file_id)
    file_url = f'https://api.telegram.org/file/bot{bot_token}/{file_info.file_path}'

    # Загрузка фотографии в поле image экземпляра Client
    with NamedTemporaryFile(delete=True) as temp_file:
        temp_file.write(request.urlopen(file_url).read())
        temp_file.flush()
        client.image.save(f'{user.telegram_id}_photo.jpg', File(temp_file))

    client.save()

    bot.send_message(message.chat.id, 'Телефон и фотография успешно добавлены.')
    bot.send_message(message.chat.id, 'Для продолжения нажми на: /categories', 
                     reply_markup=types.ReplyKeyboardRemove())



@bot.message_handler(commands=['delete'])
def delete_user(message):
    if not User.objects.filter(telegram_id=message.chat.id).exists():
        bot.send_message(message.chat.id, 'Вы не авторизованы!')
        return
    User.objects.get(telegram_id=message.chat.id).delete()
    bot.send_message(message.chat.id, 'Вы успешно удалили аккаунт для обраной регистрации нажмите на: /register')