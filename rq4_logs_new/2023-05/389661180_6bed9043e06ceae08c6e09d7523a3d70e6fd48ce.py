from tapioca.exceptions import TapiocaException


class ConflictException(TapiocaException):
    def __init__(self, message='', client=None):
        super(ConflictException, self).__init__(message, client=client)