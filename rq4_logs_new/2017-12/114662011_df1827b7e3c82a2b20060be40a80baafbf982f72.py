import telepot

TOKEN = "API KEY"
bot = telepot.Bot(TOKEN)

def on_chat_message(msg):
        chat_id = msg['chat']['id'] #prelevo id del mittente
        msgText = msg['text'] #prelevo testo del messaggio
        #username = msg['chat']['username'] #prelevo username del mittente

        #Log su shell
        print('Command Received ' + msgText)

        firstName = msg['from']['first_name']
        lastName = msg['from']['last_name']

        print('Message from: ' + firstName + " " + lastName )
        print(msg)

        if 'Ciao Luca Rossi' in msgText:
                bot.sendMessage(chat_id, ("Tara no!"))


bot.message_loop({'chat': on_chat_message}, run_forever=True)