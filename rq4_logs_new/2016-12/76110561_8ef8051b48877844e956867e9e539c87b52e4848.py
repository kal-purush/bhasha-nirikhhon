"""
1.Beste Jos,

Sint is godverdorie al 5 dagen terug naar huis! Gezeik elke keer jullie. Sint wil je toch eens even met je praten vanuit het hoofdkwartier in Spanje. Gezien de inspanningen om naar een papierloos kantoor te gaan kwam technologiepiet met deze oplossing. 't Is even wennen hoor, allemaal. 

Goed, Jos. Er ligt dus een cadeautje voor je klaar in de buurt, maar eh, technologiepiet heeft nog niet helemaal in de gaten hoe dat nou werkt met cybersecurity, en... Als ik het je zo zeg gaat de eerste de beste ermee vandoor. Dat kunnen we natuurlijk niet hebben. Ik moet wel zeker weten dat jij het bent. Het zou niet de eerste keer zijn dat iemand wordt gecatfisht door een oude man met een baard. Daar past Sint wel voor op.

We gaan het als volgt doen. Sint heeft drie raadsels die alleen te beantwoorden zijn door jou omdat het elementen uit je leven betreft die belangrijk zijn, of omdat er een behendig brein aan te pas komt. Beantwoord de vraag goed, dan kun je door naar de volgende. We beginnen met een opwarmertje. Succes!

2.Ik ben zwart als de nacht

houd je op tot 't ochtendgloren wacht

Ik pas in elke voeg en spleet

ik ben nu koud maar was heel heet.

Wat is mijn naam?

2.
Dat is 'm! Volgende! Nu een persoonlijke vraag.

Sint zoekt een fenomeen dat zich kenmerkt door ongebreidelde groei. Als meta-ondernemer is het de gemeenschappelijke deler van je werk. Welk woord spreken we liever niet hardop uit?


3.
Lees tot op de laatste letter deze laatste test. De echte zoektocht begint hierna. De virtuele raadsels zijn bijna af. 

Je geschenk ligt ergens in of rondom dit huisje. Bij je zoon's favoriete spel. Je ziet het uiteindelijk op tv. Heb je nog meer hints nodig of lukt het zo?

Je bent gesteld op je autonomie. Als je er echter niet uitkomt, is het toegestaan als een gezinslid mede-oplost. Het is niet Sint's bedoeling dat de rest van de avond wordt besteed aan dit spinnenweb. Sint stelt voor dat je opstaat en gaat verkennen met behulp van logica. Tot volgend jaar en succes met het laatste obstakel.

"""

#!/usr/bin/env python
# -*- coding: utf-8 -*-
#
# Simple Bot to reply to Telegram messages
# This program is dedicated to the public domain under the CC0 license.
"""
This Bot uses the Updater class to handle the bot.

First, a few callback functions are defined. Then, those functions are passed to
the Dispatcher and registered at their respective places.
Then, the bot is started and runs until we press Ctrl-C on the command line.

Usage:
Example of a bot-user conversation using ConversationHandler.
Send /start to initiate the conversation.
Press Ctrl-C on the command line or send a signal to the process to stop the
bot.
"""

from telegram import ReplyKeyboardMarkup
from telegram.ext import (Updater, CommandHandler, MessageHandler, Filters, RegexHandler,
                          ConversationHandler)

import logging
import time
import os 

# Enable logging
logging.basicConfig(format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
                    level=logging.INFO)

logger = logging.getLogger(__name__)

RIDDLE_1, RIDDLE_2 = range(2)




def sint(bot, update):
    update.message.reply_text("""
        Beste Jos,

        Sint is godverdorie al 5 dagen terug naar huis! Gezeik elke keer jullie. Sint wil je toch eens even met je praten vanuit het hoofdkwartier in Spanje. Gezien de inspanningen om naar een papierloos kantoor te gaan kwam technologiepiet met deze oplossing. 't Is even wennen hoor, allemaal. 

        Goed, Jos. Er ligt dus een cadeautje voor je klaar in de buurt, maar eh, technologiepiet heeft nog niet helemaal in de gaten hoe dat nou werkt met cybersecurity, en... Als ik het je zo zeg gaat de eerste de beste ermee vandoor. Dat kunnen we natuurlijk niet hebben. Ik moet wel zeker weten dat jij het bent. Het zou niet de eerste keer zijn dat iemand wordt gecatfisht door een oude man met een baard. Daar past Sint wel voor op.

        We gaan het als volgt doen. Sint heeft drie raadsels die alleen te beantwoorden zijn door jou omdat het elementen uit je leven betreft die belangrijk zijn, of omdat er een behendig brein aan te pas komt. Beantwoord de vraag goed, dan kun je door naar de volgende. We beginnen met een opwarmertje. Succes!""")
    time.sleep(2)
    update.message.reply_text("Ik ben zwart als de nacht\n"
        "houd je op tot 't ochtendgloren wacht\n"
        "Ik pas in elke voeg en spleet\n"
        "Ik ben nu koud maar was heel heet.\n"
        "Wat is mijn naam?")
    return RIDDLE_1


def riddle_1(bot, update):
    text = update.message.text
    if 'koffie' in text.lower():
        update.message.reply_text("Dat is 'm! Volgende! Nu een persoonlijke vraag.")
        update.message.reply_text("Sint zoekt een fenomeen dat zich kenmerkt door ongebreidelde groei. Als meta-ondernemer is het de gemeenschappelijke deler van je werk. Welk woord spreken we liever niet hardop uit?")
        return RIDDLE_2
    update.message.reply_text('Dat is het niet...')
    return RIDDLE_1
    
def riddle_2(bot,update):
    text = update.message.text
    if 'kanker' in text.lower():
        update.message.reply_text("""Lees tot op de laatste letter deze laatste test. De echte zoektocht begint hierna. De virtuele raadsels zijn bijna af. 

            Je geschenk ligt ergens in of rondom dit huisje. Bij je zoon's favoriete spel. Je ziet het uiteindelijk op tv. Heb je nog meer hints nodig of lukt het zo?

            Je bent gesteld op je autonomie. Als je er echter niet uitkomt, is het toegestaan als een gezinslid mede-oplost. Het is niet Sint's bedoeling dat de rest van de avond wordt besteed aan dit spinnenweb. Sint stelt voor dat je opstaat en gaat verkennen met behulp van logica. Tot volgend jaar en succes met het laatste obstakel.
            """)
        return ConversationHandler.END
    update.message.reply_text('Dat is niet het woord dat ik zoek.')
    return RIDDLE_2


def error(bot, update, error):
    logger.warn('Update "%s" caused error "%s"' % (update, error))


def cancel(bot, update):
    user = update.message.from_user
    logger.info("User %s canceled the conversation." % user.first_name)
    return ConversationHandler.END

def main():
    # Create the Updater and pass it your bot's token.
    updater = Updater(os.environ['token'])

    # Get the dispatcher to register handlers
    dp = updater.dispatcher

    # Add conversation handler with the states GENDER, PHOTO, LOCATION and BIO
    conv_handler = ConversationHandler(
        entry_points=[CommandHandler('start', sint)],

        states={

            RIDDLE_1: [MessageHandler(Filters.text,
                                           riddle_1),
                            ],

            RIDDLE_2: [MessageHandler(Filters.text,
                                          riddle_2),
                           ],
        },

        fallbacks=[CommandHandler('cancel', cancel)]
    )

    dp.add_handler(conv_handler)

    # log all errors
    dp.add_error_handler(error)

    # Start the Bot
    updater.start_polling()

    # Run the bot until the you presses Ctrl-C or the process receives SIGINT,
    # SIGTERM or SIGABRT. This should be used most of the time, since
    # start_polling() is non-blocking and will stop the bot gracefully.
    updater.idle()


if __name__ == '__main__':
    main()
