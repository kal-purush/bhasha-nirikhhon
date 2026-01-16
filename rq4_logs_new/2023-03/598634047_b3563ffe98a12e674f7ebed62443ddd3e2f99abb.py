from aiogram.types import InlineKeyboardMarkup, InlineKeyboardButton


def tatoo_and_permanent_inline_button(text_command,text, creator):
    tatoo_inline_button = InlineKeyboardButton(text=f'{text}', callback_data=f'{text_command}_{creator}')
    tatoo_inline_markup = InlineKeyboardMarkup(row_width=1)
    return tatoo_inline_markup.add(tatoo_inline_button)


def color_or_zone_inline_button(text_command, direction,creator,colors_or_zone):
    inline_markup = InlineKeyboardMarkup(row_width=1)
    for i in colors_or_zone:
        inline_button = InlineKeyboardButton(text=f'{i}', callback_data=f'{text_command}_{direction}_{creator}_{i}')
        inline_markup.add(inline_button)
    return inline_markup