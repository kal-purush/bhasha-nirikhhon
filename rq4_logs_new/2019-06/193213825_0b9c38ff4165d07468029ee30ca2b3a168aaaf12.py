#begin yandex speechkit import
import grpc
import yandex.cloud.ai.stt.v2.stt_service_pb2 as stt_service_pb2
import yandex.cloud.ai.stt.v2.stt_service_pb2_grpc as stt_service_pb2_grpc
#end yandex speechkit import


import asyncio
import json
import logging
import websockets

import urllib
import urllib.request
import urllib.parse
#import time
#from apscheduler.scheduler import Scheduler
import queue as queue
from threading import Thread
from time import sleep
import base64


CHUNKS = {}

API_KEY='AQVN34_yEdZJee7cfPzS6K9YdteELO09HLrSuUTT'

# def wait_for_chunks(websocket):
#     while ( not websocket in CHUNKS ) or ( CHUNKS[websocket]['previous'] + 1 >= len( CHUNKS[websocket]['collection'] ) ):
#         time.sleep(1)
#     CHUNKS[websocket]['previous'] += 1
#     return CHUNKS[websocket]['collection'][ CHUNKS[websocket]['previous'] ]

def get_chunk(websocket):
    try:
        while True:
#            print("decode")
            decoded = base64.b64decode(CHUNKS[websocket].get(timeout=10))
#            print("decoded:")
            #print(decoded)
            yield decoded
    except:
        pass

def out_chunk(websocket):
#    print("thread started")
    #sleep(4)
#    print("start iterate")
    for i in get_chunk(websocket):
        print(i)
#        print("now sleeping for 4 seconds")
        #sleep(4)
#        print("wake up")
#    print("thread finished")


def gen(websocket,folder_id=''):
    # Задать настройки распознавания.
#    print("gen method begin")
    specification = stt_service_pb2.RecognitionSpec(
        language_code='ru-RU',
        profanity_filter=True,
        model='general',
        partial_results=True,
        audio_encoding='OGG_OPUS',
        #sample_rate_hertz=8000
    )
    streaming_config = stt_service_pb2.RecognitionConfig(specification=specification, folder_id=folder_id)
#    print("streaming config created")
    # Отправить сообщение с настройками распознавания.
    yield stt_service_pb2.StreamingRecognitionRequest(config=streaming_config)

#    print("start iterating chunks")

    # Прочитать аудиофайл и отправить его содержимое порциями.
    for data in get_chunk(websocket):
#        print("chunk got, make recognition request")
        yield stt_service_pb2.StreamingRecognitionRequest(audio_content=data)
#        print("another recognition request was done")


def recognition(websocket, iam_token_or_api_key, folder_id=''):
    # Установить соединение с сервером.
#    print("run method begin")
#    print("wait for 5 seconds")
    #sleep(5)
#    print("waiting finished, now begin")
    cred = grpc.ssl_channel_credentials()
    channel = grpc.secure_channel('stt.api.cloud.yandex.net:443', cred)
    stub = stt_service_pb2_grpc.SttServiceStub(channel)
    authorization_type = 'Api-Key' if folder_id == '' else 'Bearer'
    # Отправить данные для распознавания.
#    print("calling gen method")
    req = gen(websocket, folder_id)
#    print("gen method executed")

    #print(next(req))
    #print(next(req))
    it = stub.StreamingRecognize(req, metadata=(('authorization', '%s %s' % (authorization_type, iam_token_or_api_key)),))
#    print("before iteration")
    # Обработать ответы сервера и вывести результат в консоль.
    try:
        for r in it:
            try:
                print('Start chunk: ')
                for alternative in r.chunks[0].alternatives:
                    print('alternative: ', alternative.text)
                print('Is final: ', r.chunks[0].final)
                print('')
            except LookupError:
                print('Not available chunks')
    except grpc._channel._Rendezvous as err:
        print('Error code %s, message: %s' % (err._state.code, err._state.details))

def synthesis(text, iam_token_or_api_key, folder_id=''):
    authorization_type = 'Api-Key' if folder_id == '' else 'Bearer'
    parameters = { 'text' : text, 'lang' : 'ru-RU', 'format' : 'oggopus' }
    if(folder_id != ''):
        parameters['folderId'] = folder_id
    req = urllib.request.Request( url='https://tts.api.cloud.yandex.net/speech/v1/tts:synthesize', data=urllib.parse.urlencode(parameters).encode("utf-8") )
    req.add_header('Authorization', '%s %s' % (authorization_type, iam_token_or_api_key))
    with urllib.request.urlopen(req) as f:
        return str(base64.b64encode(f.read()),'utf-8')

def add_chunk(websocket,chunk):
    need_init = not websocket in CHUNKS
#    print("add chunk method begin")
    if need_init:
#        print("creating queue")
        CHUNKS[websocket] = queue.Queue()
#    print("put chunk in queue")
    CHUNKS[websocket].put(chunk)
#    print("putting done")
    if need_init:
        #sched = Scheduler()
        #sched.start()
        #print("create generator")
#        print("creating thread")
        thread = Thread(target = recognition, args = (websocket,API_KEY, ))
#        print("starting thread")
        thread.start()
#        print("thread was started")
        #thread.join()
        #sched.add_interval_job(out_chunk, seconds = 5)

async def router(websocket, path):
    # register(websocket) sends user_event() to websocket

    #register(websocket)
    try:
#        print("router method begin")
        async for message in websocket:
#            print("we got message")
            data = json.loads(message)
            if data['action'] == 'recognize':
#                print("action is recognize")
                add_chunk(websocket,data['voice'])
            elif data['action'] == 'synthesize':
                base64_encoded_voice = synthesis(data['text'], API_KEY)
                
                #print('base64_encoded_voice')
                #print(base64_encoded_voice)
                print(type(base64_encoded_voice))
                print('to_send')
                to_send = json.dumps({'action': 'synthesize', 'id': data['id'], 'text': data['text'], 'voice': base64_encoded_voice, 'format': 'oggopus', 'encoding': 'base64'})
                print(to_send)
                
                
                await websocket.send(to_send)
            else:
                logging.error(
                    "unsupported event: {}", data)
    finally:
        #await unregister(websocket)
        print('finish')
print("Starting...")
asyncio.get_event_loop().run_until_complete(websockets.serve(router, 'localhost', 6789))
asyncio.get_event_loop().run_forever()