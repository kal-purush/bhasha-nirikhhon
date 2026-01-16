class SocketMock:
    def __init__(self, message: str):
        self._message = message.encode()