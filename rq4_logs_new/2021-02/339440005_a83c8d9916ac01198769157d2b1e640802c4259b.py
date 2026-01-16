from aiogram import Bot, types
from aiogram.dispatcher import Dispatcher
from aiogram.utils import executor
from aiogram.types import ReplyKeyboardRemove, \
    ReplyKeyboardMarkup, KeyboardButton, \
    InlineKeyboardMarkup, InlineKeyboardButton
import asyncio
import emoji
import time
import csv


from BD import Add_new_customer,check_account
from bill import QiwiPay,check_bill,kill_bill
from Parser_main import main
from city import cities
import key


token = key.TOKEN
bot = Bot(token=token)
dp = Dispatcher(bot=bot)



@dp.message_handler(commands=["start"])
async def cmd_start(message: types.Message):
    poll_keyboard = types.ReplyKeyboardMarkup(resize_keyboard=True)
    poll_keyboard.add(types.KeyboardButton(text="Зарегистрироваться и выбрать тариф"))
    poll_keyboard.add(types.KeyboardButton(text="Заказать парсинг"))
    await message.answer("Нажмите на кнопку ниже!", reply_markup=poll_keyboard)



@dp.message_handler(lambda message: message.text == "Зарегистрироваться и выбрать тариф")
async def action_cancel(message: types.Message):
        Add_new_customer(str(message.from_user.id),message.text,0)
        poll_keyboard = types.ReplyKeyboardMarkup(resize_keyboard=True)
        poll_keyboard.add(types.KeyboardButton(text="1"))
        poll_keyboard.add(types.KeyboardButton(text="2"))
        poll_keyboard.add(types.KeyboardButton(text="3"))
        await message.answer("1-тариф vip(безлимитное количество парса)-2500р в месяц\n 2 - тариф стандарт(30 парсов за месяц) - 300р в месяц\n 3 - единоразовый парс - 50р!", reply_markup=poll_keyboard)

@dp.message_handler(lambda message: message.text == '1' or message.text == '2' or message.text =='3')
async def action_cancel(message: types.Message):
    try:
        url_bill=""
        if message.text == '1':
            url_bill=QiwiPay(str(message.from_user.id),2500,message.text)
        elif message.text == '2':
            url_bill=QiwiPay(str(message.from_user.id),300,message.text)
        elif message.text == '3':
            url_bill=QiwiPay(str(message.from_user.id),50,message.text)    
        poll_keyboard = types.ReplyKeyboardMarkup(resize_keyboard=True)
        poll_keyboard.add(types.KeyboardButton(text="Оплатил"))
        poll_keyboard.add(types.KeyboardButton(text="Отменить"))
        await message.answer("Оплатить\n"+url_bill,reply_markup=poll_keyboard)
    except:
        await message.answer("Попробуйте снова!")

@dp.message_handler(lambda message: message.text == "Оплатил")
async def action_cancel(message: types.Message):
    try:
        ret = check_bill(str(message.from_user.id))
        poll_keyboard = types.ReplyKeyboardMarkup(resize_keyboard=True)
        if ret=="EXPIRED" or ret=="WAITING":
            poll_keyboard.add(types.KeyboardButton(text="Оплатил"))
            await message.answer("Вы не оплатили!",reply_markup=poll_keyboard)
        else:
            poll_keyboard.add(types.KeyboardButton(text="Заказать парсинг"))
            await message.answer("Отлично\nТеперь можешь заказать парсинг",reply_markup=poll_keyboard)
    except:
        await message.reply("Вы не заказывали оплату!")

@dp.message_handler(lambda message: message.text == "Отменить")
async def action_cancel(message: types.Message):
    try:
        kill_bill(str(message.from_user.id))
        remove_keyboard = types.ReplyKeyboardRemove()
        await message.answer("Всего хорошего",reply_markup=remove_keyboard)
    except:
        await message.reply("Вы не заказывали оплату!")


@dp.message_handler(lambda message: message.text == "Заказать парсинг")
async def action_cancel(message: types.Message):
    remove_keyboard = types.ReplyKeyboardRemove()
    await message.answer("Отправьте название города и чего надо спарсить!\n например: Казань откатные ворота", reply_markup=remove_keyboard)


@dp.message_handler(commands=['help'])
async def process_help_command(message: types.Message):
    await message.reply("напиши /start")


@dp.message_handler()
async def process_file_command(message: types.Message):
   # try:
        if len(message.text)>300:
            raise MyFancyException
        string=message.text.split()
        city=string[0] 
        del string[0]
        search=' '.join(string)
        try:
            g_city=cities[city]
        except:
            g_city=""
        if(len(g_city)==0):
            await message.reply("город не найден, это может быть связано с неправильным названием города, или город слишком маленький")
        else:
            check_account_st=check_account(str(message.from_user.id))
            check_account_st=str(check_account_st[0])
            check_account_st=int(check_account_st[1:-2])
            print(check_account_st)
            if check_account_st==1 or check_account_st==2 or check_account_st==3 : 
                if check_account_st==3:
                    await message.reply("Отлично,теперь ждите результата\n (В среднем занимает 10 минут)")
                    Add_new_customer(str(message.from_user.id),message.text,0)
                    user_id = message.from_user.id
                    main(g_city,search,user_id)
                    file_=search+user_id+'_'+".csv"
                    TEXT_FILE = open(file_, 'rb')
                    await bot.send_document(user_id, TEXT_FILE,
                                        caption='Держи файл!')
                    TEXT_FILE.close()
                else: 
                    await message.reply("Отлично,теперь ждите результата\n (В среднем занимает 10 минут)")
                    Add_new_customer(str(message.from_user.id),message.text,check_account_st)
                    user_id = (message.from_user.id)
                    main(g_city,search,str(user_id))
                    file_=search+str(user_id)+'_'+".csv"
                    TEXT_FILE = open(file_, 'rb')
                    await bot.send_document(user_id, TEXT_FILE,
                                        caption='Держи файл!')
                    TEXT_FILE.close()
            else:
                await message.reply("вы не оплатили подписку!")
    #except:
       # await message.reply("Попробуйте снова!")
if __name__ == '__main__':
    executor.start_polling(dp,skip_updates=True)



