from aiogram import Bot
from aiogram.types import Message

async def get_start(message: Message, bot: Bot):
    await bot.send_message(message.from_user.id, f"Привет, {message.from_user.full_name}")

async def get_photo(message: Message, bot: Bot):
    await message.answer(f"Ты отправил мне картинку? Спасибо. Сохраню ее себе")
    file = await bot.get_file(message.photo[-1].file_id)
    await bot.download_file(file.file_path, 'photo_from_bot.png')

async def get_text(message: Message, bot: Bot):
    await message.answer(f"Что это за текст?")