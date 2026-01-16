
import time
import random
import datetime
import telepot
from telepot.loop import MessageLoop

"""
After **inserting token** in the source code, run it:

```
$ python2.7 diceyclock.py
 ```

[Here is a tutorial](http://www.instructables.com/id/Set-up-Telegram-Bot-on-Raspberry-Pi/)
teaching you how to setup a bot on Raspberry Pi. This simple bot does nothing
but accepts two commands:
- `/roll` - reply with a random integer between 1 and 6, like rolling a dice.
- `/time` - reply with the current time, like a clock.
"""

pizza_list = [
'Buffalo',
'Calzone',
'Chicken BBQ',
'Chicken cheese',
'Chorizo',
'Delicatessen',
'Hot fever',
'Merguez',
'Regina',
'Royal',
'Special',
'Vegetarienne',
'Fajitas',
'Oceane',
'4 Fromages',
'Chevre miel',
'Grand large',
'Mythic burger',
'Raclette',
'Rustique au chevre',
'Steak roquefort',
'Tartiflette',
'Dauphinoise',
'Magret cepes',
'Pizza cheezy s',
'Savoyarde',
'Primavera',
'2 Saumons',
'Fois gras',
'Margherita']

def handle(msg):
    chat_id = msg['chat']['id']
    command = msg['text']

    print 'Got command: %s' % command
    if command == '/roll':
        bot.sendMessage(chat_id, random.randint(1,6))
    elif command == '/time':
        bot.sendMessage(chat_id, str(datetime.datetime.now()))
    elif command == '/pizza':
	pizza_num = random.randint(1,30)
        bot.sendMessage(chat_id, pizza_list[pizza_num])


bot = telepot.Bot('*** API Token ***')

MessageLoop(bot, handle).run_as_thread()
print 'I am listening ...'

while 1:
   time.sleep(10)