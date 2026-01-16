from googletrans import Translator
from random import randint
import twitter
import time
import re

translator = Translator()

languages = ['ar', 'bg', 'bs', 'ca', 'cs', 'cy', 'da', 'de', 'el', 'en', 'eo', 'es', 'et', 'fa', 'fi', 'ga', 'gd', 'hi', 'ht', 'hu', 'hy', 'id', 'is', 'it', 'ja', 'ka', 'kk', 'km', 'ko', 'ku', 'la', 'lb', 'lo', 'lt', 'lv', 'ml', 'mn', 'ms', 'my', 'ne', 'nl', 'pl', 'pt', 'ro', 'ru', 'sk', 'sl', 'sm', 'so', 'su', 'sv', 'sw', 'th', 'tr', 'uk', 'vi', 'yi']
phrases = ['The light has been turned on', 'And God said, "Let there be light," and there was light', 'Give light, and the darkness will disappear of itself', 'Light itself is a great corrective', 'Light is to darkness what love is to fear; in the presence of one the other disappears', 'Travel light, live light, spread the light, be the light', 'Light must come from inside. You cannot ask the darkness to leave; you must turn on the light']
attr = ['Anonymous', 'Genesis 1:3', 'Desiderius Erasmus', 'James A. Garfield', 'Marianne Williamson', 'Yogi Bhajan', 'Sogyal Rinpoche']
filein = open("oldestTweet", "r")
oldest = filein.read()
filein.close()

oldest = int(re.search(r'\d+', oldest).group())
    
while True:
    status = api.GetMentions(1, oldest)
    if status:
        status = status[0]
        #print(status)
        handle = status.user.screen_name
        oldest = status.id
        mention = status.text
        print("Mention: %s\nID: %d\n" % (mention, oldest))
        if "@lucsmacl" in mention and "lights" in mention.lower():
            lIndex = randint(0, len(languages) - 1)
            pIndex = randint(0, len(phrases) - 1)
            #print(languages[lIndex])
            #print(phrases[pIndex])

            translation = translator.translate(phrases[pIndex], dest=languages[lIndex])

            print("%s in %s" % (phrases[pIndex], languages[lIndex]))

            tweet = '@%s %s -- %s' % (handle, translation.text, attr[pIndex])

            print(tweet)
            fileout = open("oldestTweet", "w")
            writeout = "%d" % oldest
            fileout.write(writeout)
            fileout.close()
            api.PostUpdate(tweet)
    time.sleep(90)