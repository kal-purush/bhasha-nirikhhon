from aiogram.types import InlineKeyboardButton

def back_btn_element(where_call):
    # kb = InlineKeyboardBuilder()
    return InlineKeyboardButton(text="Назад", callback_data = where_call)



def back_btn(data) -> InlineKeyboardMarkup:#editusername_
    kb = InlineKeyboardBuilder()
    buttons = [InlineKeyboardButton(text="Назад", callback_data=data)]
    kb.add(*buttons, back_btn)
    kb.adjust(2)
    return kb.as_markup(resize_keyboard=True)