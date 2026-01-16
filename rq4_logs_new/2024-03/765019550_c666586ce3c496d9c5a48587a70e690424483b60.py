from telebot import types
from apps.bot.settings import bot
from apps.bot.services.carts import cart_service



@bot.callback_query_handler(func=lambda call: call.data.startswith('buy_'))
def add_product_cart(call):
    """
    Добавление товара в корзину
    product_id = id продукта переданный в callback_data
    tg_id = id пользователя
    cart = корзина пользователя
    cart_item = продукт корзины
    """
    product_id = int(call.data.split('_')[1])
    tg_id = call.from_user.id
    cart = cart_service.get_cart(tg_id)
    cart_item = cart_service.add_product(cart, product_id, 1)
    if cart_item:
        bot.send_message(call.message.chat.id, 'Товар добавлен в корзину!')
        bot.delete_message(call.message.chat.id, call.message.message_id)
        return
    bot.send_message(call.message.chat.id, 'Не удолось добавить товар в корзину!')





@bot.message_handler(commands=['cart'])
def show_cart(message):
    """
    Показать корзину пользователя
    """
    tg_id = message.from_user.id
    cart = cart_service.get_cart(tg_id)
    if cart:
        items = cart_service.get_cart_items(cart)
        markup = types.InlineKeyboardMarkup()
        for item in items:
            markup.add(types.InlineKeyboardButton(
                text=item.product.name,
                callback_data=f'cartitem_{item.product.id}'))
        bot.send_message(message.chat.id, 'Ваши товары: ', reply_markup=markup)

    else:
        bot.send_message(message.chat.id, 'Корзина пуста!')
        bot.delete_message(message.chat.id, message.message_id)
        return

