import requests

url = 'https://api.telegram.org/bot'
token = '609015270:AAEFnNJwGdYpi49_Vw_SQZRxIwCV5aREl3w'


def get_bot_updates(limit, offset):
    req = url + token + '/getUpdates'
    par = {'limit': limit, 'offset': offset}
    result = requests.get(req, params=par)
    # форматируем json в словарь
    decoded = result.json()
    return decoded['result']


def sendMessageBot(text, chat_id):
    req = url + token + '/sendMessage'
    par = {'chat_id': chat_id, 'text': text}
    requests.post(req, params=par)


def get_coin_rub(coin):
    param = coin + '-rub'
    req = 'https://api.cryptonator.com/api/ticker/'+param
    result = requests.get(req)
    decoded = result.json()
    result = decoded['ticker']
    return result['price']


while True:
    try:
        result = get_bot_updates(100, 0)
        for item in result:
            update_id = item['update_id']
            new_offset = update_id + 1
            result = get_bot_updates(5, new_offset)
            chat_id = item['message']['chat']['id']
            text = item['message']['text']
            if text == '/start':
                text = 'Привет! Я суперпупер бот, который покажет тебе стоимоть бетховенов и эфира в деревянных =)\nСписок команд:\n - /btc - цена за бетховен\n - /eth - цена за эфир'
                sendMessageBot(text, chat_id)
            elif text == '/btc':
                coin = get_coin_rub('btc')
                text = 'бетховены опять упали, стоят ' + coin + ' руб.'
                sendMessageBot(text, chat_id)
            elif text == '/eth':
                coin = get_coin_rub('eth')
                text = 'эфир сегодня ' + coin + ' руб.'
                sendMessageBot(text, chat_id)
            else:
                continue
    except KeyboardInterrupt:  # порождается, если бота остановил пользователь
        print('Interrupted by the user')
        break