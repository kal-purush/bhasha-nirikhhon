#import sys
import time
import telepot     
from apiclient.discovery import build
#from apiclient.errors import HttpError
import argparse
import json



#Configurazione per interrogare le API di Google
DEVELOPER_KEY = "AIzaSyD7UmSpz9MUsk1FX0nCuz3Pfzkc9ffsSjY"
YOUTUBE_API_SERVICE_NAME = "youtube"
YOUTUBE_API_VERSION = "v3"

#Configurazione BOT
TOKEN='360912478:AAFFP864KdWdybNreoohQfyO-4u1C34O3io'
bot=telepot.Bot(TOKEN)

commandOk=False




#Funzione per leggere messaggi inviati al BOT
def commands(msg):
    content_type, chat_type, chat_id = telepot.glance(msg)
    if content_type == 'text':
        messaggio=msg['text']    
        if messaggio == "/getSong":
            commandOk=True
        else:
            bot.sendMessage(chat_id ,'Comandi disponibili: /getSong')
    else:
        bot.sendMessage(chat_id ,'Accetto solo messaggi di testo!')


def song(msg):
    content_type, chat_type, chat_id = telepot.glance(msg)
    if content_type == 'text':
        messaggio=msg['text']
        argparser = argparse.ArgumentParser()
        argparser.add_argument("--q", help="Search term", default=messaggio)
        argparser.add_argument("--max-results", help="Max results", default=1)
        args = argparser.parse_args()    
        #chiama la funzione con i parametri impostati
        youtube_search(args)
        videoID = youtube_search(args)
        bot.sendMessage(chat_id ,'https://www.youtube.com/watch?v='+videoID)
        commandOk=False

#Funzione che interroga le API di Google e ritorna le info del video più popolare
def youtube_search(options):
    youtube = build(YOUTUBE_API_SERVICE_NAME, YOUTUBE_API_VERSION,
                    developerKey=DEVELOPER_KEY)

    search_response = youtube.search().list(
        q=options.q,
        part="id,snippet",
        maxResults=options.max_results
    ).execute()

    risposta = json.dumps(search_response)

    risposta=json.loads(risposta)


    return risposta['items'][0]['id']['videoId']      


                  
#Programma principale
if __name__ == "__main__":    
    bot.getUpdates()   

    if commandOk== False:
        r=bot.message_loop(commands)
    else:
          bot.message_loop(song)

# Keep the program running.
while 1:
    time.sleep(10)