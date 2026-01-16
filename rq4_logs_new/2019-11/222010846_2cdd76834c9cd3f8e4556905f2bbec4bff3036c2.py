#!/usr/bin/env python3
# encoding: utf-8

import os
import socket


def socket_connection_factory():
    client_socket = socket.socket()
    client_socket.connect((os.environ['KOTOMERA_VIDEO_URL'], 8888))
    return client_socket.makefile('wb')