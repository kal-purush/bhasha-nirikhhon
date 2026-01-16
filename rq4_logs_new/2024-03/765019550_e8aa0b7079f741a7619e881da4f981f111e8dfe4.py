from telebot import types
from apps.customers.models import Adress
from apps.bot.settings import bot    
from apps.bot.services.deliveries import delivery_service




@bot.callback_query_handler(func=lambda call: call.data.startswith('order'))
def order(call: types.CallbackQuery):
    tg_id = call.from_user.idАдресс
    adresses = Adress.objects.filter(customer__user__telegram_id=tg_id)
    if not adresses:
        ...

    



@bot.callback_query_handler(func=lambda call: call.data.startswith('deliverycart_'))
def create_delivery(call):
    cart_id = int(call.data.split('_')[1])

    delivery_service.create_delivery()



# card -> Заказать -> Адресс -> Дата и Время  