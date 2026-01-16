import socket
import socketserver
import threading
import time
import zlib
from datetime import datetime
from socketserver import TCPServer, ThreadingMixIn, UDPServer
import os

import msgpack
import redis

# REMOTE_REDIS_HOST_DEBUG = '192.168.1.14'

VERSION = "0.2"
DASH_LINE = "-----------------------------------------------------------------"

configuration = {
    "TRANSMISSION_INTERVAL": 1,
    "COMPRESSION_TYPE": 1,
    "REMOTE_REDIS_HOST": "",
    "REMOTE_REDIS_PORT": 6379,
    "REMOTE_REDIS_DB": 8,
    "LOCAL_HOST": "0.0.0.0",
    "LOCAL_UDP_PORT_SYSLOG": 514,
    "LOCAL_TLS_PORT_SYSLOG": 6514
}

one_second = 1000000000  # 1 sec = 1000000000 nanoseconds

beginning_of_time_interval = ""
data_block = {"dt": [], "ip": [], "endpoint": [], "raw_message": []}
data_increment = 0
endpoint_name = ""


def load_env_variable(env_variable):
    global configuration
    env_variable_value = os.getenv(env_variable)
    if env_variable_value:
        print(env_variable_value)
        configuration[env_variable] = env_variable_value
    return


def load_configuration():
    global configuration

    try:
        configuration['REMOTE_REDIS_HOST'] = os.getenv('REMOTE_REDIS_HOST')
        # if configuration['REMOTE_REDIS_HOST'] is None:
        #     configuration['REMOTE_REDIS_HOST'] = REMOTE_REDIS_HOST_DEBUG
        load_env_variable("TRANSMISSION_INTERVAL")
        load_env_variable("COMPRESSION_TYPE")
        load_env_variable("REMOTE_REDIS_HOST")
        load_env_variable("REMOTE_REDIS_PORT")
        load_env_variable("REMOTE_REDIS_DB")
        load_env_variable("LOCAL_HOST")
        load_env_variable("LOCAL_UDP_PORT_SYSLOG")
        load_env_variable("LOCAL_TLS_PORT_SYSLOG")
    except Exception:
        print(DASH_LINE)
        print("ERROR: REMOTE_REDIS_HOST enviroment variable is not available.")
        print(DASH_LINE)
        raise


def redis_connect(redis):
    redis_connection = redis.Redis(
        host=configuration["REMOTE_REDIS_HOST"],
        port=configuration["REMOTE_REDIS_PORT"],
        db=configuration["REMOTE_REDIS_DB"])
    return redis_connection


def break_pri(pos):
    for facility in range(24):
        for severity in range(8):
            pri = (facility * 8) + severity
            if pos[0] == str(pri):
                return facility, severity
    return 0, 0


class SyslogHandler(socketserver.BaseRequestHandler):
    def handle(self):
        global beginning_of_time_interval
        global data_block
        global endpoint_name
        global configuration

        raw_message_data = bytes.decode(self.request[0].strip())

        socket = self.request[1]

        dt = datetime.now().isoformat(timespec='microseconds')

        print(raw_message_data)

        if (time.monotonic_ns() -
                beginning_of_time_interval) < send_data_interval:
            if str(raw_message_data).strip() != "":
                data_block["dt"].append(dt)
                data_block["ip"].append(self.client_address[0])
                data_block["raw_message"].append(str(raw_message_data))
                data_block["endpoint"].append(endpoint_name)
        else:
            packed_message = msgpack.packb([data_block], use_bin_type=True)
            packed_message = msgpack.packb([data_block], use_bin_type=True)
            compressed_message = zlib.compress(
                packed_message, configuration["COMPRESSION_TYPE"])
            r.rpush('raw_message_block', compressed_message)
            beginning_of_time_interval = time.monotonic_ns()
            data_block = {
                "dt": [],
                "ip": [],
                "endpoint": [],
                "raw_message": []
            }


class ThreadingUDPServer(ThreadingMixIn, UDPServer):
    pass


class ThreadingTCPServer(ThreadingMixIn, TCPServer):
    pass


if __name__ == "__main__":
    try:

        load_configuration()
        send_data_interval = configuration["TRANSMISSION_INTERVAL"] * \
            one_second
        local_host = configuration["LOCAL_HOST"]
        endpoint_name = socket.getfqdn()
        print(DASH_LINE)
        print("Narwhal endpoint v." + VERSION + " on " + endpoint_name)

        server = []
        server_thread = []

        r = redis_connect(redis)

        try:
            response = r.client_list()
            connected_to_redis = True
            print("Connected to Redis. Listening on ports:")
            print(configuration["LOCAL_UDP_PORT_SYSLOG"])
            print(configuration["LOCAL_TLS_PORT_SYSLOG"])
            print(DASH_LINE)

        except redis.ConnectionError:
            connected_to_redis = False
            print(DASH_LINE)
            print("ERROR: The Redis server is unavailable.")
            print("Please check configuration or availability of the server.")
            print(DASH_LINE)
        if connected_to_redis:
            endpoint_name = socket.getfqdn()
            beginning_of_time_interval = time.monotonic_ns()

            server.append(
                UDPServer((local_host, configuration["LOCAL_UDP_PORT_SYSLOG"]),
                          SyslogHandler))
            server.append(
                TCPServer((local_host, configuration["LOCAL_TLS_PORT_SYSLOG"]),
                          SyslogHandler))

            for server_ref in server:
                server_thread.append = threading.Thread(
                    target=server_ref.serve_forever(poll_interval=0.5))

            for server_thread_ref in server_thread:
                server_thread_ref.daemon = True
                server_thread_ref.start()

            while True:
                continue

    except (IOError, SystemExit):
        raise
    except KeyboardInterrupt:
        print("Shutting down endpoint service...")
        for server_ref in server:
            server_ref.server_close()
        print('done.')