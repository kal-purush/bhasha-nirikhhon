import socket
import threading
import json
import api_requests
from time import sleep
import os
from event_handlers import handle_request, public_ip_change
from utils import get_public_ip
import upnp

settings = None


def init_background_threads(s):
    global settings
    settings = s


# main thread waits for a request from decentorage node (audit/download/upload)
def listen_for_req():
    print("waiting for requests on port ", settings.decentorage_port)
    server_socket = socket.socket()
    server_socket.bind((settings.local_ip, settings.decentorage_port))
    server_socket.listen(5)
    while True:
        connection, addr = server_socket.accept()
        request = connection.recv(1024).decode("utf-8")
        request = json.loads(request)
        request_thread = threading.Thread(target=handle_request, args=(request,))
        request_thread.start()


# thread to send heartbeat to decentorage, scheduled every 5 seconds
def heart_beat():
    while True:
        api_requests.send_heart_beat()
        sleep(5)

'''
# thread to track changes in ip
def track_ip():
    global public_ip, local_ip
    while True:
        p_ip = get_public_ip()
        if p_ip != public_ip:
            public_ip_change()

        l_ip = upnp.get_my_ip()
        if l_ip != local_ip:
            local_ip_change(l_ip)

        sleep(10)
'''


# thread that is scheduled every 12 hours to request withdraw
# send shard id to decentorage, decentorage checks if host should be paid or not
def withdraw():
    shards = os.listdir(settings.data_directory)
    for shard in shards:
        api_requests.withdraw_request(shard)

    # send withdraw every 12 hours
    sleep(12*60*60)