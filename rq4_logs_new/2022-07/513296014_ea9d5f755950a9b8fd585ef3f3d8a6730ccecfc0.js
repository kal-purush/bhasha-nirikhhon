require('dotenv').config()
console.log(process.env.TOKEN)

const TelegramBot = require('node-telegram-bot-api');
const TOKEN = process.env.TOKEN;

const bot = new TelegramBot(TOKEN, {
    polling:true
});

bot.on('message', (msg)=>{
    const chatId = msg.chat.id

    if(msg.text === 'Технічна Підтримка') {
            bot.sendMessage(chatId, 'Контакти', {
                reply_markup: {
                    inline_keyboard: [
                         [
                            {
                                text: 'Телефон',
                                callback_data: 'phone'
                             },
                             {
                                text: 'Пошта',
                                callback_data: 'email'
                             },
                            {
                                text: 'геолокація',
                                callback_data: 'geo',
                             },
                         ]
                     ]
                }
            })
        }else if(msg.text === 'Замовити таксі'){
            bot.sendMessage(chatId, 'Тип таксі', {
                reply_markup: {
                    inline_keyboard: [
                         [
                            {
                                text: 'Економ',
                                callback_data: 'econom'
                             },
                             {
                                text: 'Комфорт',
                                callback_data: 'komfort'
                             },
                            {
                                text: 'Бізнес',
                                callback_data: 'biznes',
                             },
                         ]
                     ]
                }
            })
        }else{
            bot.sendMessage(chatId, 'Привіт '+msg.from.first_name,{
                reply_markup: {
                    keyboard: [
                        ['Технічна Підтримка', 'Замовити таксі']
                    ],
                    resize_keyboard: true
                }
            })
        }
           
})
    
  


bot.on('callback_query', query => {
    if (query.data == 'phone') {
        bot.sendContact(query.message.chat.id, '+380983397634', 'Vitalik',{
            last_name:'Buchma'
        })
    }
     else if (query.data == 'geo') {
        bot.sendLocation(query.message.chat.id, '49.557785','23.300235',)
    }else if(query.data == 'email'){
        bot.sendMessage(query.message.chat.id, 'vitalikbuchma@gmail.com')
    }
})