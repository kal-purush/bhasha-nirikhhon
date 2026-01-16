import socketserver
from Sources.Shared.Logger import Logger


class PCSocketConnection(socketserver.BaseRequestHandler):
    """
    The RequestHandler class for our server.

    It is instantiated once per connection to the server, and must
    override the handle() method to implement communication to the
    client.
    """

    def __init__(self, request, client_address, server):
        self.pc_server = server.pc_server
        self.data = request.recv(1024).strip()
        self.logger = Logger.Logger(self.__class__.__name__)
        super().__init__(request, client_address, server)

    def handle(self):
        # self.request is the TCP socket connected to the client
        # print "{} wrote:".format(self.client_address[0])
        self.logger.debug("ClientIP:", self.client_address[0])
        self.logger.debug("Rx: ", self.data)
        # just send back the same data, but upper-cased
        # self.request.sendall(self.data.upper())
        reply = bytes(self.data).decode(encoding='UTF-8').upper()
        self.logger.debug("Tx: ", reply)
        self.request.sendall(str.encode(reply, 'UTF-8'))

        self.logger.debug("handle, data: ", self.data)
        self.logger.debug("PCServer: ", self.pc_server)
        self.pc_server.on_received_connection_data("", self.data)