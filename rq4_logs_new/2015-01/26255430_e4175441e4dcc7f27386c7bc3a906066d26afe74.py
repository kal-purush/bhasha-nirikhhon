
class Chatroom(object):
    ''' Responsible for handling the communication between
        clients in the chatroom (a Mediator class).
    '''

    def __init__(self):
        self.clients = list()

    def Clients(self):
        return self.clients

    def AddClient(self, client):
        self.clients.append(client)
        print '%s joined the chat room.' % client.Name()

    def SendMessageToAll(self, sender, message):
        for client in self.clients:
            if client == sender:
                continue
            client.AddReceivedMessage('%s: %s' % (sender.Name(), message))
        print '%s sent the following to all users: \"%s\"' % (sender.Name(), message)