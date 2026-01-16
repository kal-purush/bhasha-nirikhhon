import random
import telebot;

bot = telebot.TeleBot('1669576677:AAHC06TJeVvf361EHEEU-em2UcJUHDPGSAs')

@bot.message_handler(regexp='бухать|план б|план|плнб')
def respond_to_party_invitation(message):
    responses = [
        "не, я в климах",
        "-",
        "давай на следующей неделе",
        "я уже бухаю",
        "не сегодня. я сегодня делаю предложение",
    ]
    bot.send_message(message.chat.id, random.choice(responses))

@bot.message_handler(regexp='климы|климовичи|климах|климовичах')
def respond_klimovichi(message):
    if "погода" in message.text.lower():
        bot.send_message(message.chat.id,
            ("Тут должна быть инфа из гугла, но я так и не заимплементил это.\n"
            "Поэтому в климах сейчас безветренно"))
        return

    if ["вернешься", "вернетесь", "вернусь"] in message.text.lower():
        bot.send_message(message.chat.id, "на следующей неделе стопудова")

@bot.message_handler(regexp='андрей|Андрей|бухарик|Бухарик|бот|романенко|Романенко')
def respond_personal(message):
    if "как дела" in message.text.lower():
        responses = ["заебись", "а у тебя?", "недождетесь", "ахуенно. я бухаю"]
        bot.send_message(message.chat.id, random.choice(responses))
        return

    if any(w for w in ["где ты", "ты где"] if w in message.text.lower()):
        responses = ["в климах", "в климовичах", "в запое", "а как ты думаешь?)"]
        bot.send_message(message.chat.id, random.choice(responses))
        return

if __name__ == '__main__':
     bot.polling(none_stop=True)