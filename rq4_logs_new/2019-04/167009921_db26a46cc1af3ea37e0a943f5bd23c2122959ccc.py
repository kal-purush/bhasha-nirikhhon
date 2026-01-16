import os.path
from synergistic import poller, broker

c = broker.client.Client("nescio.culley.me", 8891, "Storage")


def write(channel, msg_id, payload):
    print(channel, msg_id, payload)
    with open(payload['filename'], 'a') as file:
        file.write(payload.get('content', '') + '\r\n')

def read(channel, msg_id, payload):
    if not os.path.isfile(payload['filename']):
        c.respond(msg_id, 'error file does not exist')
        return
    with open(payload['filename'], 'r+') as file:
        lines = file.readlines()
    c.respond(msg_id, lines)



poller = poller.Poll(catch_errors=False)
poller.add_client(c)
c.subscribe('storage.write', write)
c.subscribe('storage.read', read)
poller.serve_forever()