import logging
import telegram
from telegram.error import NetworkError, Unauthorized
from time import sleep
import random
import json
from random import randint

# Enable logging
logging.basicConfig(format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
                    level=logging.INFO)

logger = logging.getLogger(__name__)
update_id = None
sticker = ['CAADAgADXgcAAnlc4gneIyGzwWLmPQI', 'CAADAQADFwADyIsGAAF1CK9t7qjAigI', 'CAADAQADIQADyIsGAAHaCFln7THl9QI',
           'CAADAgADYwADECECECX9ZCfAKlspAg',  'CAADAgADuwUAAvoLtggGKjKfVlb_hAI', 'CAADAQADIAADyIsGAAGwI-I5pMSEdQI',
           'CAADAQADLgADyIsGAAGPsGcNmlLjPQI']

categorias = ['Saludos', 'Pregunta', 'Chanceo', 'Exclamacion', 'Photo_in', 'Photo', 'Afirmacion', 'Negacion', 'Advervios']

advervios = ['como', 'cómo']

cuanti_pos = ['mucho', 'mucha', 'varios', 'varias', 'bastantes']
cuanti_neg = ['pocos', 'escasos']

acciones = ['hacer', 'ver', 'tener', 'comer']

agradecimientos = ['gracias']

peticion = ['quieres', 'te gustaria', 'quisieras', 'quiero']

saludos = ['epa', 'hola', 'que hubo', 'hey', 'hello']
despedidas = ['chao', 'adios', 'bye', 'luego']

calificaciones = ['bien', 'excelente', 'avanzando', 'mejor']

afirmativos = ['si', 'ok', 'esta bien']
negaciones = ['no']

interrogativos = ['?', '¿']
exclamaciones = ['!', '¡']

chanceos = ['mami', 'bella', 'amor', 'beso']

photo = ['photo', 'imagen', 'foto', 'pic']

files = []
photos = []

youtube = ['https://youtu.be/JcI8yn7xQWo']

preguntas = ['¿como estas?', '¿que haces?', '¿como puedo ser de utilidad?']

despectivos = ['¿me parece que te confundiste de persona?', '¿creo que no sabes que soy un bot?',
               '¿no entiendo que respuesta esperas?', 'entiendo tu confusion... pero no!']

NoneType = type(None)
bs = " "
respuesta = ""


def main():
    global update_id
    # Telegram Bot Authorization Token
    bot = telegram.Bot('347715594:AAFxTVbmmV1pLhXAmnXLd72XWnxyYxqwlvE')

    # get the first pending update_id, this is so we can skip over it in case
    # we get an "Unauthorized" exception.
    try:
        update_id = bot.get_updates()[0].update_id

    except IndexError:
        update_id = None

    while True:
        try:
            bot_resp(bot)
        except NetworkError:
            sleep(1)
        except Unauthorized:
            # The user has removed or blocked the bot.
            update_id += 1


def bot_resp(bot, respuesta=""):
    global update_id
    # Request updates after the last update_id
    for update in bot.get_updates(offset=update_id, timeout=10):
        update_id = update.update_id + 1
        if update.message:  # your bot can receive updates without messages
            # Reply to the message
            logger.info("New message " + "from: " + str(update.message.chat.id))
            # logger.info(json.dumps(str(update.message), sort_keys=False, indent=4, separators=(',', ': ')))
            recep = update.message.chat.first_name
            tags = []

            if type(update.message.sticker) is not NoneType:
                tags.append('Sticker')
                logger.info(update.message.sticker)
                sticker.append(update.message.sticker.file_id)
                respuesta += random.choice(despectivos) + bs
                bot.send_sticker(update.message.chat.id, sticker[randint(0, len(sticker)-1)])

            if len(update.message.photo) is not 0:
                photos.append(update.message.photo)
                logger.info(update.message.photo)
                tags.append('Photo_in')

            if update.message.text is not None:
                for signo in interrogativos:
                    if 'Pregunta' not in tags:
                        if update.message.text.find(signo) != -1:
                            tags.append('Pregunta')

                frase = update.message.text.lower().split(' ')

                for palabra in frase:
                    for saludo in saludos:
                        if 'Saludos' not in tags:
                            if palabra.find(saludo) != -1:
                                tags.append('Saludos')

                    for advervio in advervios:
                        if 'Advervios' not in tags:
                            if palabra.find(advervio) != -1:
                                tags.append('Advervios')

                    for chanceo in chanceos:
                        if 'Chanceo' not in tags:
                            if palabra.find(chanceo) != -1:
                                tags.append('Chanceo')

            cn_tag = 0
            for tag in tags:
                logger.info("Revisando tag: " + tag)
                if respuesta not in saludos:
                    if tag == 'Saludos':
                        respuesta += random.choice(saludos) + bs + recep
                        if 'Advervios' in tags:
                            respuesta += ','
                        respuesta += bs

                if respuesta not in saludos:
                    if tag == 'Chanceo':
                        respuesta += random.choice(despectivos) + bs
                        bot.send_sticker(update.message.chat.id, sticker[randint(0, len(sticker)-1)])

                if respuesta not in calificaciones:
                    if tag == 'Advervios':
                        respuesta += random.choice(calificaciones) + bs + random.choice(agradecimientos) + bs
                        if recep not in respuesta:
                            respuesta += recep + ',' + bs
                        respuesta += random.choice(preguntas) + bs

                if respuesta not in photo:
                    if tag == 'Photo_in':
                        logger.info("Creo respuestas Photo_in")
                        respuesta += recep + bs + random.choice(agradecimientos) + bs + "por la" + bs + \
                                     random.choice(photo)

                cn_tag += 1
            if cn_tag == 0:
                logger.info(update.message.text)
                logger.warning("there is no category on this message")
                respuesta += random.choice(despectivos) + bs
                bot.send_sticker(update.message.chat.id, sticker[randint(0, len(sticker) - 1)])

            update.message.reply_text(respuesta)
            logger.info("Response SEND: " + respuesta)
            # update.message.reply_text(youtube)

if __name__ == '__main__':
    logger.info("Starting - BOT")
    main()