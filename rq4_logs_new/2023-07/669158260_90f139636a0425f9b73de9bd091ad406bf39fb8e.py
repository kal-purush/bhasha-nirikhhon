from aiogram.types import ReplyKeyboardMarkup, KeyboardButton

kb_client = ReplyKeyboardMarkup()
b1 = KeyboardButton('/start')
b = KeyboardButton('/help')
b2 = KeyboardButton('/sigma')
b3 = KeyboardButton('/toxi$')
b4 = KeyboardButton('/maryday')
b5 = KeyboardButton('/cancel')
b6 = KeyboardButton('Привет Антох, вопрос по механике доты...')


kb_client.row(b1, b)
kb_client.row(b2, b3)
kb_client.row(b4, b5)
kb_client.add(b6)

