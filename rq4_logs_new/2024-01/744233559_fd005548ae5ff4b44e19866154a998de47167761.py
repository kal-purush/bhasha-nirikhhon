from aiogram import Router, F, types
from aiogram.filters import Command
from aiogram.fsm.state import State, StatesGroup
from aiogram.fsm.context import FSMContext


opros_router = Router()

class Opross(StatesGroup):
    name = State()
    age = State()
    phone = State()
    your_favorite_anime = State()



@opros_router.message(Command("start_opros"))
async def start_registration(message: types.Message, state: FSMContext):
    await state.set_state(Opross.name)
    await message.answer(
        "Прелагаем вам пройти опрос! Можете остановить регистрацию командой /cancel")
    await message.answer("Как Вас зовут?")


@opros_router.message(Command("cancel"))
@opros_router.message(F.text.lower() == "отмена")
async def cancel_registration(message: types.Message, state: FSMContext):
    await state.clear()
    await message.answer("Опрос отменён!")


@opros_router.message(Opross.name)
async def process_name(message: types.Message, state: FSMContext):
    await state.update_data(name=message.text)
    await state.set_state(Opross.age)
    await message.answer("Сколько Вам лет?")


@opros_router.message(Opross.age)
async def process_age(message: types.Message, state: FSMContext):
    age = int(message.text)
    await state.update_data(age=age)
    await state.set_state(Opross.your_favorite_anime)
    await message.answer("Какое ваше любимое аниме?")


@opros_router.message(Opross.your_favorite_anime)
async def process_napravlenie(message: types.Message, state: FSMContext):
    await state.update_data(anime=message.text)
    await state.set_state(Opross.phone)
    await message.answer("Ваш номер телефона?")


@opros_router.message(Opross.phone)
async def process_phone(message: types.Message, state: FSMContext):
    await state.update_data(phone=message.text)
    data = await state.get_data()
    await message.answer(f"Ваши данные: {data}")
    await message.answer("Поздравляем! Вы прошли опрос!")

    await state.clear()