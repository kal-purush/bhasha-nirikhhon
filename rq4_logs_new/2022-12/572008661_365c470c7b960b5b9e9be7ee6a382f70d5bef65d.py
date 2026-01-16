import datetime

import telegram
from django.conf import settings


def create_message(return_date, book_name, user_email):
    borrow_date = datetime.date.today()
    return (f"New borrowing was created. \n"
            f"Borrow date:{borrow_date} \n"
            f"Expected return date:{return_date} \n"
            f"Book: {book_name} \n"
            f"User: {user_email}")


def send_message(text):
    token = settings.BOT_TOKEN
    chat_id = settings.BOT_CHAT_ID
    bot = telegram.Bot(token=token)
    bot.sendMessage(chat_id=chat_id, text=text)