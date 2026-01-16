import telebot
from telebot.types import (
    InlineKeyboardMarkup,
    InlineKeyboardButton,
    KeyboardButton,
    ReplyKeyboardMarkup
)
import config
import keyboards
from keyboards import ReplyKB, InlineKB
from models import models


bot = telebot.TeleBot(config.TOKEN)

@bot.message_handler(commands=['start'])
def start(message):
    greeting_str = 'Hi'

    keyboard = ReplyKB().get_beginning_kb()
    bot.send_message(message.chat.id, greeting_str, reply_markup=keyboard)

@bot.message_handler(func=lambda message: message.text == keyboards.beginning_kb['products'])
def show_categories(message):
    """
    :param message:
    :return: listed root categories
    """
    category_queryset = models.Category.get_root_categories()
    kb = InlineKB(
        iterable=category_queryset,
        lookup_field='id',
        named_arg='category',
    )

    bot.send_message(message.chat.id, "Выберите категорию", reply_markup=kb.gen_kb())

@bot.callback_query_handler(func=lambda call: call.data.split('_')[0] == 'category')
def show_products_or_sub_category(call):
    """
    :param call:
    :return: listed subcategories || listed products
    """
    obj_id = call.data.split('_')[1]
    category = models.Category.objects(id=obj_id).get()
    if category.is_parent:
        kb = InlineKB(
            iterable=category.subcategory,
            lookup_field='id',
            named_arg='category'
        )
        bot.edit_message_text(text=category.title,chat_id=call.message.chat.id,
                              message_id=call.message.message_id,
                              reply_markup=kb.gen_kb())
    else:
        print("NON PARENT")


@bot.callback_query_handler(func=lambda m: m.data.split('_')[0] == 'back')
def back(call):
    id = call.data.split('_')[1]
    category = models.Category.objects(id=id).get()

    if category.is_root:
        category_queryset = models.category.get_root_categories()
        kb = InlineKB(
            iterable=category_queryset,
            lookup_field='id',
            named_arg='category'
        )
        kb.gen_kb()
    else:
        kb = InlineKB(
            iterable=category.parent.subcategory,
            lookup_field='id',
            named_arg='category'
        )
        kb.gen_kb()
        kb.add(InlineKeyboardButton(text=f'<< {category.cparent.title}',
            callback_data=f'back_{category.parent.id}'))

    text = 'Категории' if not category.is_root else category.parent.title
    bot.edit_message_text(text=text, chat_id=call.message.chat.id,
        message_id=call.message.message_id,
        reply_markup=kb)


if __name__ == '__main__':
    bot.polling(none_stop=True)