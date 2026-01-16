# -*- coding: utf-8 -*-

from DB.DBAlchemy import DBManager
from settings.message import MESSAGES
from .handler import Handler


class HandlerInlineQuery(Handler):
    """
    Класс обрабатывает входящие текстовые сообщения от нажатия на инлайн
    кнопки

    """
    def __init__(self, bot):
        super(HandlerInlineQuery, self).__init__(bot)
        self.BD = DBManager()

    def pressed_btn_product(self, call, code):
        """
        Обрабатывает входящие запросы на нажатие кнопок товара inline
        """
        self.bot.answer_callback_query(
            call.id,
            MESSAGES['product_order'].format(
                self.BD.select_single_product_name(code),
                self.BD.select_single_product_title(code),
                self.BD.select_single_product_price(code),
                self.BD.select_single_product_quantity(code)),
            show_alert=True)

    def handle(self):
        """Обработчик(декоратор) запросов от нажатия на кнопки товара"""
        @self.bot.callback_query_handler(func=lambda call: True)
        def callback_inline(call):
            code = call.data
            if code.isdigit():
                code = int(code)

            self.pressed_btn_product(call, code)