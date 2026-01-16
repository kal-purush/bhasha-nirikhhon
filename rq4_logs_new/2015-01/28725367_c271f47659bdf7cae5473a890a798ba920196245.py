# -*- coding: utf-8 -*-
__doc__ = """
Support for ws4py in waitress
"""

from ws4py.websocket import Heartbeat, WebSocket as _WebSocket
from waitress.channel import HTTPChannel

__all__ = ['WebSocket', 'EchoWebSocket']


class WebSocket(_WebSocket):

    def __init__(self, sock, protocols=None, extensions=None,
                 environ=None, heartbeat_freq=None):
        _WebSocket.__init__(self, sock, protocols, extensions,
                            environ, heartbeat_freq)
        if self.heartbeat_freq:
            self.hb = Heartbeat(self, self.heartbeat_freq)

    def once(self):
        """
        Make it clear this won't be used as
        IO runs off asyncore.loop() in server
        """
        raise NotImplemented()

    def run(self):
        """
        Make it clear this won't be used as
        IO runs off asyncore.loop() in server
        """
        raise NotImplemented()

    def _write(self, data):
        """
        self.sock must be an instance of waitress' WSHTTPChannel
        so use WSHTTPChannel.write_soon() to queue data to be sent
        """
        if self.terminated or self.sock is None:
            raise RuntimeError("Cannot send on a terminated websocket")

        self.sock.write_soon(data)

    def close_connection(self):
        """
        Close the underlying transport
        """
        self.sock.will_close = True

    def opened(self):
        """
        Called by the server when the upgrade handshake has succeeded.
        If overriding in subclass, please call this as follows ...
        WebSocket.opened(self)
        """
        if self.heartbeat_freq:
            self.hb.start()

    def closed(self, code, reason=None):
        """
        Called when the websocket stream and connection are finally closed.
        If overriding in subclass, please call this as follows ...
        WebSocket.closed(self, code, reason)
        """
        if self.heartbeat_freq:
            self.hb.stop()


class EchoWebSocket(WebSocket):
    def received_message(self, message):
        """
        Automatically sends back the provided ``message`` to
        its originating endpoint.
        """
        self.send(message.data, message.is_binary)


class WSHTTPChannel(HTTPChannel):
    """
    Support switching from HTTPChannel to WebSocket
    """
    def __init__(self, server, sock, addr, adj, map=None):
        """
        Override HTTPChannel.__init__ just to initialise _websocket
        """
        self._websocket = None
        HTTPChannel.__init__(self, server, sock, addr, adj, map)

    def websocket_opened(self, websocket):
        """
        Called from WSGITask when handshake is successful
        """
        if websocket:
            websocket.opened()
            self._websocket = websocket

    def received(self, data):
        """
        If not a websocket, behave as a normal HTTPChannel
        If a websocket, then process received data as such
        """
        if self._websocket:
            return self._websocket.process(data)
        else:
            return HTTPChannel.received(self, data)

    def handle_close(self):
        """
        Override HTTPChannel.handle_close() just to
        to call websocket's terminate() so it can clean up
        """
        HTTPChannel.handle_close(self)
        if self._websocket:
            self._websocket.terminate()