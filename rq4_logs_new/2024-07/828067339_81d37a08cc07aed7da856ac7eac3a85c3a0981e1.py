import asyncio
from aiogram import Bot, Dispatcher, F
from aiogram.types import (Message, CallbackQuery,
                           InlineKeyboardMarkup, InlineKeyboardButton,
                           ReplyKeyboardMarkup, KeyboardButton,
                           URLInputFile)
from aiogram.filters import CommandStart
from aiogram.fsm.state import State, StatesGroup
from aiogram.fsm.context import FSMContext

main_btn = ReplyKeyboardMarkup(keyboard=[
    [KeyboardButton(text='🍔Burgerlar')]
], resize_keyboard=True)

burger_narx, count = 0, 0

class Phone(StatesGroup):
    number = State()

def get_menu():
    return InlineKeyboardMarkup(inline_keyboard=[
        [InlineKeyboardButton(text='➖', callback_data='minus'),
         InlineKeyboardButton(text=str(count), callback_data='narx'),
         InlineKeyboardButton(text='➕', callback_data='plus')],
        [InlineKeyboardButton(text=str(burger_narx)+' sum', callback_data='umumiy')],
        [InlineKeyboardButton(text='✅Zakaz berish', callback_data='zakaz')]
    ])

cancel_btn = ReplyKeyboardMarkup(keyboard=[
    [KeyboardButton(text='❌Bekor qilish')]
], resize_keyboard=True)

API_TOKEN = '7499834470:AAHgNGZXMcLPIvwMb2gVR6kMElLa0nOv9qs'#Bot Father bergan bot api tokeni
ADMIN_ID = 7499834470 #admin id sini yozing faqat uzini

bot = Bot(token = API_TOKEN)
dp = Dispatcher()

@dp.message(CommandStart())
async def start(message: Message):
    await message.answer('🔽O\'zingizga kerakli bo\'limni tanlang:',
                         reply_markup=main_btn)

@dp.message(F.text == '🍔Burgerlar')
async def burger(message: Message):
    await message.answer_photo(photo=URLInputFile('https://www.foodandwine.com/thmb/pwFie7NRkq4SXMDJU6QKnUKlaoI=/1500x0/filters:no_upscale():max_bytes(150000):strip_icc()/Ultimate-Veggie-Burgers-FT-Recipe-0821-5d7532c53a924a7298d2175cf1d4219f.jpg'),
                               caption=f'🍔Kamol Burger\n\n💸Narxi: {35000} sum',
                               reply_markup=get_menu())

@dp.callback_query(F.data == 'minus')
async def minus(callback: CallbackQuery):
    global burger_narx, count
    if burger_narx > 0 and count > 0:
        burger_narx -= 35000
        count -= 1
    await callback.answer()
    await callback.message.edit_reply_markup(reply_markup=get_menu())

@dp.callback_query(F.data == 'plus')
async def plus(callback: CallbackQuery):
    global burger_narx, count
    burger_narx += 35000
    count += 1
    await callback.answer()
    await callback.message.edit_reply_markup(reply_markup=get_menu())

@dp.callback_query(F.data == 'zakaz')
async def zakaz(callback: CallbackQuery, state: FSMContext):
    await callback.answer("✅Buyurtmangiz qabul qilindi!")
    await callback.message.answer(f"✅Sizning buyurtmangiz\n\n🍔Kamol Burger {count} ta\n💸Jami: {burger_narx} sum\n\n📞Siz bilan bog'lanishimiz uchun telefon raqamingizni yozib yuboring!",
                                  reply_markup=cancel_btn)
    await state.set_state(Phone.number)

@dp.message(Phone.number)
async def cmd_number(message: Message, state: FSMContext):
    global burger_narx, narx
    await state.update_data(phone=message.text)
    if message.text == '❌Bekor qilish':
        await message.answer('Menyu', reply_markup=main_btn)
    else:
        await message.answer('Rahmat, operator siz bilan tez orada bog\'lanadi', reply_markup=main_btn)
        await message.bot.send_message(ADMIN_ID, f'✅Zakaz qabul qilindi\n\n👤{message.from_user.full_name}\n🍔Kamol Burger: {count} ta\n💸Narxi {burger_narx} sum\n📞{message.text}')
        with open("zakazlar.txt", "a") as file:
            file.write(f"{message.from_user.full_name} | {count} | {burger_narx} | {message.text} ")
    burger_narx, narx = 0, 0
    await state.clear()

async def main():
    await dp.start_polling(bot)

if __name__ == '__main__':
    try:
        asyncio.run(main())
    except KeyboardInterrupt:
        print('Stop')