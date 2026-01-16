import aiohttp
import aiofile
import json
import time
import asyncio
import websockets
import logging
import sys
from runsafe import runSafe as execCode

# Configure Logging Utility
logging.basicConfig(filename='pyBot.log', filemode='w', level=logging.DEBUG,
                    format='%(levelname)s:%(name)s:%(asctime)s:%(message)s')
wslogger = logging.getLogger('websockets')
wslogger.setLevel(logging.DEBUG)
wslogger.addHandler(logging.StreamHandler())

logger = logging.getLogger('bot')
logger.setLevel(logging.DEBUG)

# session data
session = None

def fake(json):
    pass
# make http request using method, path, params, body, and pass result to consumer
# method = '', path = [], params = {}, body = {}, consumer = function({})
async def makeAioRequest(method, path, params=None, body=None, consumer=fake):
    global session
    req = 'https://discordapp.com/api/v6'
    for s in path:
        req += f'/{s}'
    logger.info('Sending https request %s %s', method, path)
    async with session.request(method, req, params=params, json=body) as res:
        try:
            res_json = await res.json()
            consumer(res_json)
            return res_json
        except:
            consumer({})
            return {}

# convience function to synchronously start an asynchronous makeAioRequest
def sendMessage(method, path, params=None, body=None, consumer=fake):
    try:
        asyncio.create_task(makeAioRequest(method, path, params=params, body=body, consumer=consumer))
    except:
        loop.create_task(makeAioRequest(method, path, params=params, body=body, consumer=consumer))

# convience methods for common http requests
httpapi = {
    'message': lambda ch, content: sendMessage('POST', ['channels', ch, 'messages'], body={'content': content}),
    'react': lambda ch, mes, emoji: sendMessage('POST', ['channels', ch, 'messages', mes, 'reactions', emoji, '@me']),
    'emojis': lambda guild, consumer: sendMessage('GET', ['guilds', guild, 'emojis'], consumer=consumer),
}


def createPayload(opCode, data):
    return {
        'op': opCode,
        'd': data
    }


# check to see if discord responded to heartbeat message
receivedACK = False


async def heartbeat(ws, heartbeatInterval):
    global receivedACK
    global state
    while True:
        await asyncio.sleep(heartbeatInterval)
        logger.info('Heartbeat seq: %s', state['seq'])
        if not receivedACK:
            logger.warning('Connection Lost: No ACK')
        await ws.send(json.dumps(createPayload(1, state['seq'])))
        receivedACK = False

# stored for potential resume
state = {
    'time': '',
    'sessionId': '',
    'seq': None,
}


async def writeState():
    async with aiofile.AIOFile('state.json', 'w') as s:
        state['time'] = time.time()
        await s.write(json.dumps(state))


# list of guilds connected to (populated by GUILD_CREATE events)
guilds = {}


async def handleEvent(type, data):
    global guilds
    global state
    # global send
    if type == 'GUILD_CREATE':
        guilds[data['id']] = data
        logger.info('Added Guild: %s', data['name'])
    elif type == 'READY':
        state['sessionId'] = data['session_id']
        logger.info('Ready, Version: %s', data['v'])
    elif type == 'GUILD_UPDATE':
        guilds[data['id']] = {guilds[data['id']], data}
    elif type == 'GUILD_DELETE':
        guilds.pop(data['id'], None)
    elif type == 'MESSAGE_CREATE':
        logger.info('Message Recieved: %s', data['content'])
        try:
            if data['content'].startswith('```'):
                con = data['content'].strip('`\n')
                i = con.index('\n')
                logger.info(f'Running code "{con[i+1:]}"')
                execCode(con[:i], con[i+1:], lambda c:httpapi['message'](data['channel_id'], f'{con[:i]}\n```\n{c}\n```'))
        except:
            logger.info('Error when running')
    else:
        logger.info('Unknown type: %s', type)
    asyncio.create_task(writeState())
    return


async def identify(ws, data):
    global secret
    heartbeatInv = (data['heartbeat_interval'] - 10)/1000
    logger.info('Heartbeat Interval: %d', heartbeatInv)
    # send identity payload (op 2, payload is in secret.json)
    # TODO add resume option
    await ws.send(json.dumps(createPayload(2, secret['identity'])))
    # start heartbeat task AFTER identifing
    asyncio.create_task(heartbeat(ws, heartbeatInv))


async def main():
    global receivedACK
    global state
    global session
    global secret
    # Read secret.json for secret data. secret.json is gitignored to hide the token and other sensitive info
    async with aiofile.AIOFile('secret.json') as secret_json:
        secret = json.loads(await secret_json.read())
    # create http client, and set global headers
    session = aiohttp.ClientSession(headers={
        'Authorization': 'Bot '+secret['token'],
        'User-Agent': 'DiscordBot ('+secret['url']+', '+secret['version']+')',
        'Content-Type': 'application/json'
    })
    async with websockets.connect((await makeAioRequest('GET', ['gateway']))['url']+'?v=6&encoding=json') as ws:
        while True:
            try:
                raw = await ws.recv()
            except Exception as e:
                logger.warning('Exception while reading: %s', e)
                if ws.closed:
                    return
                raw = ''
            if raw != '':
                # convert raw json to data, and grab opCode to handle event
                data = json.loads(raw)
                opCode = data['op']
                if opCode == 0:
                    # update sequence number
                    state['seq'] = data['s']
                    await handleEvent(data['t'], data['d'])
                elif opCode == 1:
                    logger.info('Pinged')
                elif opCode == 7:
                    logger.warning('Reconnect')
                    return
                elif opCode == 9:
                    logger.warning('Invalid Session')
                    return
                elif opCode == 10:
                    # to avoid connection lost b/c we haven't recieved an ACK before sending a heartbeat
                    receivedACK = True
                    await identify(ws, data['d'])
                elif opCode == 11:
                    receivedACK = True
                    logger.info('Recieved ACK')
                    pass
                else:
                    logger.warning('unknown opCode: %s', opCode)
            else:
                logger.warning('No Packets')


loop = asyncio.get_event_loop()
loop.run_until_complete(main())