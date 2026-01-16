from aiogram import Bot, Dispatcher, executor, types
from aiogram.contrib.fsm_storage.memory import MemoryStorage
from aiogram.dispatcher.filters.state import State, StatesGroup
from aiogram.types import ReplyKeyboardMarkup, KeyboardButton
from aiogram.types import InlineKeyboardMarkup, InlineKeyboardButton


bot = Bot(token="***")
dp = Dispatcher(bot, storage=MemoryStorage())

kb = ReplyKeyboardMarkup(resize_keyboard=True)
btn_Calories = KeyboardButton(text="Рассчитать")
btn_Info = KeyboardButton(text="Информация")
btn_Buy = KeyboardButton(text='Купить')
kb.row(btn_Calories, btn_Info, btn_Buy)

inline_kb = InlineKeyboardMarkup()
btn_Calories = InlineKeyboardButton(text='Рассчитать норму калорий', callback_data='calories')
btn_Formulas = InlineKeyboardButton(text='Формулы расчёта', callback_data='formulas')
inline_kb.add(btn_Calories)
inline_kb.add(btn_Formulas)

inline_buy_kb = InlineKeyboardMarkup()
btn_product1 = InlineKeyboardButton(text='Product1', callback_data='product_buying')
btn_product2 = InlineKeyboardButton(text='Product2', callback_data='product_buying')
btn_product3 = InlineKeyboardButton(text='Product3', callback_data='product_buying')
btn_product4 = InlineKeyboardButton(text='Product4', callback_data='product_buying')
inline_buy_kb.row(btn_product1, btn_product2, btn_product3, btn_product4)

class UserState(StatesGroup):
    age = State()
    growth = State()
    weight = State()


@dp.message_handler(commands=['start'])
async def start(message):
    await message.answer('Начало работы бота', reply_markup=kb)


@dp.message_handler(text=['Рассчитать'])
async def main_menu(message):
    await message.answer('Выберите опцию:', reply_markup=inline_kb)


@dp.callback_query_handler(text='formulas')
async def get_formulas(call):
    await call.message.answer('Упрощенный вариант формулы Миффлина-Сан Жеора:')
    await call.message.answer('для мужчин: 10 х вес (кг) + 6,25 x рост (см) – 5 х возраст (г) + 5;')
    await call.answer()


@dp.callback_query_handler(text='calories')
async def set_age(call):
    await call.message.answer('Введите свой возраст:')
    await UserState.age.set()
    await call.answer()


@dp.message_handler(state=UserState.age)
async def set_growth(message, state):
    await state.update_data(age=message.text)
    await message.answer('Введите свой рост:')
    await UserState.growth.set()


@dp.message_handler(state=UserState.growth)
async def set_weight(message, state):
    await state.update_data(growth=message.text)
    await message.answer('Введите свой вес:')
    await UserState.weight.set()


@dp.message_handler(state=UserState.weight)
async def send_calories(message, state):
    await state.update_data(weight=message.text)
    data = await state.get_data()
    calories = 10 * int(data['weight']) + 6.25 * int(data['growth']) - 5 * int(data['age']) + 5
    await message.answer(f'результат: {calories}')
    await state.finish()


@dp.message_handler(text=['Информация'])
async def set_age(message):
    await message.answer('Какое-то описание бота')


@dp.message_handler(text=['Купить'])
async def get_buying_list(message):
    for i in range(1, 5):
        with open(f'photo/{i}.png', 'rb') as photo:
            await message.answer_photo(photo, f'Название: Product{i} | Описание: описание {i} | Цена: {i * 100}')
    await message.answer('Выберите продукт для покупки:', reply_markup=inline_buy_kb)


@dp.callback_query_handler(text='product_buying')
async def send_confirm_message(call):
    await call.message.answer('Вы успешно приобрели продукт!')
    await call.answer()


if __name__ == "__main__":
    executor.start_polling(dp, skip_updates=True)