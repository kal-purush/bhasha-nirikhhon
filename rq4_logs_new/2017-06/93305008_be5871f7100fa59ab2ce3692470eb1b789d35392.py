#import sys
import time
import telepot     
from apiclient.discovery import build
#from apiclient.errors import HttpError
import argparse
import json


#Funzione per leggere messaggi inviati al BOT
def handle(msg):
    content_type, chat_type, chat_id = telepot.glance(msg)
    #print("Tipo del contenuto: ",content_type)
    #print("Tipo di chat: ",chat_type)
    #print("ID del mittente: ",chat_id)

    if content_type == 'text':
        messaggio=msg['text']
        #if messaggio == "/getSong":
            #Parametri con cui effettuare la richiesta alle API
        argparser = argparse.ArgumentParser()
        argparser.add_argument("--q", help="Search term", default=messaggio)
        argparser.add_argument("--max-results", help="Max results", default=1)
        args = argparser.parse_args()    
        #chiama la funzione con i parametri impostati
        youtube_search(args)
        videoID = youtube_search(args)
        bot.sendMessage(chat_id ,'https://www.youtube.com/watch?v='+videoID)

       # else:
         #   bot.sendMessage(chat_id ,'Comandi disponibili: /getSong')
                

    else:
        bot.sendMessage(chat_id ,'Accetto solo messaggi di testo!')

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


    #ParConfigurazione per interrogare le API di Google
    DEVELOPER_KEY = "AIzaSyD7UmSpz9MUsk1FX0nCuz3Pfzkc9ffsSjY"
    YOUTUBE_API_SERVICE_NAME = "youtube"
    YOUTUBE_API_VERSION = "v3"

    #Setto il BOT
    TOKEN='360912478:AAFFP864KdWdybNreoohQfyO-4u1C34O3io'
    bot=telepot.Bot(TOKEN)

    resp=bot.getUpdates()
    bot.message_loop(handle)


    

# Keep the program running.
while 1:
    time.sleep(10)