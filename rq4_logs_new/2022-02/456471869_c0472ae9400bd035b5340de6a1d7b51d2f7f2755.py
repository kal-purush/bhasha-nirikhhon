"""
Функции клиента: сформировать presence-сообщение; отправить сообщение серверу;
получить ответ сервера; разобрать сообщение сервера;параметры командной
строки скрипта client.py <addr> [<port>]: addr — ip-адрес сервера;
port — tcp-порт на сервере, по умолчанию 7777.
"""

from socket import socket, AF_INET, SOCK_STREAM
from sys import argv
import json
from time import time
from common.variables import ACTION, ACCOUNT_NAME, RESPONSE, PRESENCE, TIME, USER, ERROR, DEFAULT_PORT, \
    DEFAULT_ACCOUNT_NAME, DEFAULT_IP_ADDRESS
from common.utils import get_msg, send_msg, check_port


def create_presence(account_name=DEFAULT_ACCOUNT_NAME):
    out = {
        ACTION: PRESENCE,
        TIME: time(),
        USER: {
            ACCOUNT_NAME: account_name
        }
    }
    return out


def process_ans(message):
    if RESPONSE in message:
        if message[RESPONSE] == 200:
            return '200 : ok'
        return f'400 : {message[ERROR]}'
    raise ValueError


def main():
    try:
        server_address = argv[1]
        server_port = int(argv[2])
        check_port(server_port)
    except IndexError:
        server_address = DEFAULT_IP_ADDRESS
        server_port = DEFAULT_PORT

    transport = socket(AF_INET, SOCK_STREAM)
    transport.connect((server_address, server_port))
    message_to_server = create_presence()
    send_msg(transport, message_to_server)
    try:
        answer = process_ans(get_msg(transport))
        print(answer)
    except (ValueError, json.JSONDecodeError):
        print('Не удалось декодировать сообщение сервера.')


if __name__ == '__main__':
    main()
