import telebot
from pathlib import Path
import logging
import os

TOKEN=os.environ.get('BOT_TOKEN','')
if not TOKEN:
  logging.error('No BOT_TOKEN env provided...')
  exit()
bot=telebot.TeleBot(TOKEN)
telebot.logger.setLevel(logging.DEBUG)

def get_memes() -> list:
  memes_path = Path('./memes')
  return list(memes_path.glob('*.mp3'))

@bot.message_handler(commands=['list'])
def start_message(message):
  bot.send_message(message.chat.id,"Список мемов:")
  memes_str=""
  for (i, item) in enumerate(get_memes()):
    memes_str += f"{i}. {item}\n"
  bot.send_message(message.chat.id, memes_str)


@bot.message_handler(commands=['meme'])
def start_message(message):
  if ' ' not in message.text:
    bot.reply_to(message, '/meme <i>, где i - номер мема из /list')
  i = int(message.text.split()[1])
  if i<=0:
    with open('memes\\не_понимаю.mp3', 'rb') as f:
      bot.send_voice(message.chat.id, f)
  with open(get_memes()[i], 'rb') as f:
    bot.send_voice(message.chat.id, f)

if __name__ == "__main__":
  bot.infinity_polling()