import telebot
import datetime

TOKEN = "8112218373:AAG-mpYC0_NamWnbS8d39wRKvVvjenFkb5E"  # جایگزین کنید
bot = telebot.TeleBot(TOKEN)

# دیکشنری برای ذخیره موقت داده‌ها
user_data = {}

@bot.message_handler(commands=['start'])
def send_welcome(message):
    bot.reply_to(message, "سلام! برای ثبت فیش واریزی، لطفاً نام خود را وارد کنید:")

@bot.message_handler(func=lambda m: True)
def handle_message(message):
    chat_id = message.chat.id
    if chat_id not in user_data:
        user_data[chat_id] = {"step": 1, "name": None, "sender": None, "receiver": None, "amount": None}
        bot.send_message(chat_id, "لطفاً نام خود را وارد کنید:")
    else:
        current_step = user_data[chat_id]["step"]
        if current_step == 1:
            user_data[chat_id]["name"] = message.text
            user_data[chat_id]["step"] = 2
            bot.send_message(chat_id, "نام انتقال‌دهنده را وارد کنید:")
        elif current_step == 2:
            user_data[chat_id]["sender"] = message.text
            user_data[chat_id]["step"] = 3
            bot.send_message(chat_id, "نام گیرنده را وارد کنید:")
        elif current_step == 3:
            user_data[chat_id]["receiver"] = message.text
            user_data[chat_id]["step"] = 4
            bot.send_message(chat_id, "مبلغ واریزی را به ریال وارد کنید:")
        elif current_step == 4:
            user_data[chat_id]["amount"] = message.text
            user_data[chat_id]["step"] = 5
            bot.send_message(chat_id, "لطفاً عکس فیش واریزی را ارسال کنید.")

@bot.message_handler(content_types=['photo'])
def handle_photo(message):
    chat_id = message.chat.id
    if chat_id in user_data and user_data[chat_id]["step"] == 5:
        # ذخیره اطلاعات در یک فایل متنی (در حالت واقعی باید در دیتابیس ذخیره شود)
        data = user_data[chat_id]
        with open("payments.txt", "a") as file:
            file.write(f"{datetime.datetime.now()} | {data['name']} | {data['sender']} | {data['receiver']} | {data['amount']}\n")
        
        # دانلود عکس (در حالت واقعی نیاز به ذخیره در فضای ابری دارد)
        file_id = message.photo[-1].file_id
        file_info = bot.get_file(file_id)
        downloaded_file = bot.download_file(file_info.file_path)
        
        with open(f"receipt_{chat_id}_{datetime.datetime.now().strftime('%Y%m%d_%H%M%S')}.jpg", "wb") as photo:
            photo.write(downloaded_file)
        
        bot.send_message(chat_id, "اطلاعات با موفقیت ثبت شد! متشکرم.")
        del user_data[chat_id]

bot.polling()