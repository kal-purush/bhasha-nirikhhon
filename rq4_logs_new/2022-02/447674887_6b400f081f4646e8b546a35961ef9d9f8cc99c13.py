import ipaddress
import logging

server_loger = logging.getLogger('server_logger')
client_logger = logging.getLogger('client_logger')


class ServerPort:
    """
    дескриптор для описания порта сервера. порт должен быть в диапазоне [1024:65535]
    """

    def __set__(self, instance, value):

        if value > 1023 and value < 65536:
            instance.__dict__[self.name] = value
        # проверяем откуда вызван дескриптор и кидаем ошибку в соответсвующий логгер
        # сравниваю строки, так как более нормального решения не нашел. isinstance не получается использовать,
        # так как придется импортировать MessengerClient & MessengerClient и получим ошибку об импорте друг в друга
        elif str(type(instance)) == "<class '__main__.MessengerServer'>":
            server_loger.critical(f'значение порта {value} недопустимо')
        elif str(type(instance)) == "<class '__main__.MyMessengerClient'>":
            client_logger.critical(f'значение порта {value} недопустимо')

    def __set_name__(self, owner, name):
        self.name = name


class ServerHost:
    """
    дескриптор для описания ip сервера. должен проходить проверку на ip адресс
    """

    def __set__(self, instance, value):
        try:
            ipaddress.ip_address(value)
            instance.__dict__[self.name] = value
        except Exception as e:
            # проверяем откуда вызван дескриптор и кидаем ошибку в соответсвующий логгер
            # сравниваю строчки, так как более нормального решения не нашел. isinstance не получается использовать,
            # так как придется импортировать MessengerClient & MessengerClient и получим ошибку об импорте друг в друга
            if str(type(instance)) == "<class '__main__.MessengerServer'>":
                server_loger.critical(e)
            elif str(type(instance)) == "<class '__main__.MyMessengerClient'>":
                client_logger.critical(e)
            exit(1)

    def __set_name__(self, owner, name):
        self.name = name