import telebot
import time
def convert_to10ss(number, oldss):
    counter = 0
    letter_dict = {'A': 10, 'B': 11, 'C': 12, 'D': 13, 'E': 14, 'F': 15}
    d = len(number) - 1
    for i in number:
        if i.isdigit():
            counter += int(i) * (int(oldss) ** d)
        else:
            counter += letter_dict[i] * (int(oldss) ** d)
        d -= 1
    return counter
def convert_tonewss(counter, newss):
    num_dict = {10: 'A', 11: 'B', 12: 'C', 13: 'D', 14: 'E', 15: 'F'}
    s = ''
    while counter > 0:
        p = counter % int(newss)
        if p < 10:
            s = str(p) + s
        else:
            s = num_dict[p] + s
        counter //= int(newss)
    return s
def check_number(number, oldss):
    invalid_characters = []
    for i in number:
        if i.isdigit() and int(i) >= int(oldss):
            invalid_characters += i
        elif ord(i) - int(oldss) > 54:
            invalid_characters += i
    if len(invalid_characters) > 0:
        return ','.join(invalid_characters)
    else:
        return ''
bot = telebot.TeleBot("1680788626:AAFe4kEqOUAFOnIJ7sfamtIJCIhZ-7dP5z0", parse_mode=None)
@bot.message_handler(commands=['start'])
def greeting(message):
    bot.send_sticker(message.chat.id, open('stickerfortelegram.webp', 'rb'))
    keyboard = telebot.types.ReplyKeyboardMarkup(True)
    keyboard.row('/help')
    bot.send_message(message.chat.id,
                     'Я бот для перевода числа из одной системы счисления в другую.\nВведи /help, чтобы познакомиться с моим функционалом поподробнее :)',
                     reply_markup=keyboard)
@bot.message_handler(commands=['help'])
def help(message):
    keyboard = telebot.types.ReplyKeyboardMarkup(True)
    keyboard.row('/go')
    bot.send_message(message.chat.id,
                     'Для начала введите команду /go\nЗатем укажите число\nПосле введите исходную и нужную систему счисления в диапазоне от 2 до 16.',
                     reply_markup=keyboard)
    bot.send_message(message.chat.id, 'Посмотри следующее видео и сразу все поймешь.')
    bot.send_video(message.chat.id, open('videofortelegram.mp4', 'rb'))
    time.sleep(5)
    bot.send_message(message.chat.id, 'Вот еще пример:')
    bot.send_photo(message.chat.id, open('photofortelegram.jpg', 'rb'))
@bot.message_handler(commands=['go'])
def asknumber(message):
    bot.send_message(message.chat.id, 'Введите число:')
    bot.register_next_step_handler(message, takenumber)
def takenumber(message):
    global number
    number = message.text.upper()
    bot.send_message(message.chat.id, 'Введите исходную систему счисления числа:')
    bot.register_next_step_handler(message, takeoldss)
def takeoldss(message):
    global oldss
    oldss = message.text
    bot.send_message(message.chat.id, 'Введите нужную систему счисления:')
    bot.register_next_step_handler(message, takenewss)
def takenewss(message):
    global newss
    newss = message.text
    while True:
        if len(number) > 1024:
            keyboard = telebot.types.ReplyKeyboardMarkup(True)
            keyboard.row('/go', '/help')
            bot.send_message(message.chat.id,
                             'Извини, но я не могу переводить числа, имеющие больше, чем 1024 десятка. Попробуйте еще разок.')
            bot.send_sticker(message.chat.id, open('photofortelegram3.webp', 'rb'))
            break
        if number in '/start/help/go' or oldss in '/start/help/go' or newss in '/start/help/go':
            keyboard = telebot.types.ReplyKeyboardMarkup(True)
            keyboard.row('/go', '/help')
            bot.send_message(message.chat.id, 'Вы не можете вызвать другую команду, пока не введете число, исходную и нужную систему счисления! Попробуйте еще разок.')
            bot.send_sticker(message.chat.id, open('stickerfortelegram2.webp', 'rb'))
            break
        if oldss.isdigit() != True:
            keyboard = telebot.types.ReplyKeyboardMarkup(True)
            keyboard.row('/go', '/help')
            s = oldss + ' - не натуральное число. Попробуйте еще разок'
            bot.send_message(message.chat.id, s, reply_markup=keyboard)
            bot.send_sticker(message.chat.id, open('stickerfortelegram2.webp', 'rb'))
            break
        if int(oldss) < 2 or int(oldss) > 16:
            keyboard = telebot.types.ReplyKeyboardMarkup(True)
            keyboard.row('/go', '/help')
            f = oldss + ' не принадлежит диапазону! Попробуйте еще разок.'
            bot.send_message(message.chat.id, f, reply_markup=keyboard)
            bot.send_sticker(message.chat.id, open('stickerfortelegram2.webp', 'rb'))
            break
        if newss.isdigit() != True:
            keyboard = telebot.types.ReplyKeyboardMarkup(True)
            keyboard.row('/go', '/help')
            t = newss + ' - ненатуральное число! Попробуйте еще разок.'
            bot.send_message(message.chat.id, t, reply_markup=keyboard)
            bot.send_sticker(message.chat.id, open('stickerfortelegram2.webp', 'rb'))
            break
        if int(newss) < 2 or int(newss) > 16:
            keyboard = telebot.types.ReplyKeyboardMarkup(True)
            keyboard.row('/go', '/help')
            i = newss + ' не принадлежит диапазону! Попробуйте еще разок.'
            bot.send_message(message.chat.id, i, reply_markup=keyboard)
            bot.send_sticker(message.chat.id, open('stickerfortelegram2.webp', 'rb'))
            break
        if number.isalnum() != True:
            keyboard = telebot.types.ReplyKeyboardMarkup(True)
            keyboard.row('/go', '/help')
            r = number + ' - небуквенно-символьная строка! Попробуйте еще разок.'
            bot.send_message(message.chat.id, r, reply_markup=keyboard)
            bot.send_sticker(message.chat.id, open('stickerfortelegram2.webp', 'rb'))
            break
        if len(check_number(number, oldss)) > 0:
            keyboard = telebot.types.ReplyKeyboardMarkup(True)
            keyboard.row('/go', '/help')
            if len(check_number(number, oldss)) == 1:
                u = check_number(number, oldss) + ' не принадлежит ' + oldss + ' системе счисления. Попробуйте еще разок.'
                bot.send_message(message.chat.id, u, reply_markup=keyboard)
                bot.send_sticker(message.chat.id, open('stickerfortelegram2.webp', 'rb'))
            else:
                l = check_number(number, oldss) + ' не принадлежат ' + oldss + ' системе счисления. Попробуйте еще разок.'
                bot.send_message(message.chat.id, l, reply_markup=keyboard)
                bot.send_sticker(message.chat.id, open('stickerfortelegram2.webp', 'rb'))
            break
        bot.send_message(message.chat.id, convert_tonewss(convert_to10ss(number, oldss), newss))
        keyboard = telebot.types.ReplyKeyboardMarkup(True)
        keyboard.row('/go', '/help')
        bot.send_message(message.chat.id,
                         'Введите /go, чтобы продолжить переводить числа из одной системы счисления в другую.',
                         reply_markup=keyboard)
        break
bot.polling(none_stop=True)