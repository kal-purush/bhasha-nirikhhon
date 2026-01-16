from telegram.ext import Updater, CommandHandler, MessageHandler, Filters
import os
import sqlite3
import random
quote_cache = []
conn = sqlite3.connect('quotes.db', check_same_thread=False)

def seven(bot, update):
    if update.message.text.lower().split(' ')[0] in ["7", "seven", "seven!"]:
        update.message.reply_text("Seven!")

def random_quote(bot, update):
    global quote_cache
    if len(quote_cache) > 0:
        update.message.reply_text(random.choice(quote_cache))

def create_schema():
    global conn
    c = conn.cursor()
    c.execute('CREATE TABLE IF NOT EXISTS quotes (text)')
    conn.commit()

def save_to_quotedb(message):
    global conn
    c = conn.cursor()
    c.execute('INSERT INTO quotes VALUES (?)', (message, ))
    conn.commit()

def load_quotedb():
    global conn
    global quote_cache
    c = conn.cursor()
    c.execute('SELECT * FROM quotes')
    for quote in c.fetchall():
        quote_cache.append(quote[0])

def quote(bot, update):
    global quote_cache
    try:
        quote = update.message.text.split(' ')[1]
    except IndexError:
        update.message.reply_text('No quote specified!')
        return
    save_to_quotedb(quote)
    quote_cache.append(quote)
    update.message.reply_text("Quote added!")

if __name__ == "__main__":
    create_schema()
    load_quotedb()
    updater = Updater(os.environ['TELEGRAM_API_KEY'])
    updater.dispatcher.add_handler(CommandHandler("quote", quote))
    updater.dispatcher.add_handler(CommandHandler("random_quote", random_quote))
    updater.dispatcher.add_handler(MessageHandler([Filters.text], seven))
    updater.start_polling()
    updater.idle()