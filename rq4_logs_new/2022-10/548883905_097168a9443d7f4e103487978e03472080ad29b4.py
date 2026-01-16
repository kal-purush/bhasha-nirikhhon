import serial
import logging
import sys
import requests
import json

from time import sleep, time
from datetime import datetime
from os import mkdir
from serial.tools import list_ports

PID_MICROBIT = 516
VID_MICROBIT = 3368
TIMEOUT = 0.1

BASE_URL = 'http://localhost:9000'
HEADERS = {'Content-type': 'application/json'}

# logging stuff
try:
    mkdir(".logs")
except FileExistsError:
    pass

fp = f'./.logs/{datetime.now()}.log'.replace(' ', '-')

if sys.platform.lower() == 'win32':
    fp = fp.replace(':', '.')

logger = logging.getLogger()
logger.setLevel(logging.DEBUG)

fh = logging.FileHandler(fp)
fh.setLevel(logging.DEBUG)

ch = logging.StreamHandler()
ch.setLevel(logging.INFO)

fmt = logging.Formatter('[%(asctime)s] [%(name)s] [%(levelname)s] : %(message)s')

ch.setFormatter(fmt)
fh.setFormatter(fmt)

logger.addHandler(ch)
logger.addHandler(fh)

def register(username):
    requests.post(BASE_URL + '/api/users', data = json.dumps({'name': username}), headers = HEADERS)

def submit(username, time_taken):
    requests.post(BASE_URL + '/api/entries', data = json.dumps({'name': username, 'timetaken': time_taken}), headers = HEADERS)

def find_comport(pid, vid, baud):
    """returns a serial port"""
    ser_port = serial.Serial(timeout=TIMEOUT)
    ser_port.baudrate = baud
    ports = list(list_ports.comports())
    # scanning ports
    logger.info("Scanning Ports...")
    for p in ports:
        try:
            logger.info(f'Testing: \n\t port: {p!r} \n\t pid: {p.pid} \n\t vid: {p.vid}')
        except AttributeError:
            continue

        if p.pid == pid and p.vid == vid:
            logger.info(f'found target device: \n\t pid: {p.pid} \n\t vid: {p.vid} \n\t port: {p.device}')
            ser_port.port = str(p.device)
            return ser_port
    return None

def readline(ser):
    while True:
        line = ser.readline().decode('utf-8')
        if line:
            return line

def main():
    logger.info('looking for microbit')
    ser_micro = find_comport(PID_MICROBIT, VID_MICROBIT, 115200)
    if not ser_micro:
        logger.warn('microbit not found')
        return

    logger.info('opening and monitoring microbit port')
    ser_micro.open()
    while 1:
        name = input("please enter your name: ")
        register(name)

        ser_micro.write('ready\n'.encode('utf-8'))
        logger.info("laptop -> microbit : 'ready'")

        resp = readline(ser_micro)
        logger.info(f"microbit -> laptop : '{resp}'")

        if not resp.startswith('start') and resp:
            ser_micro.write('badresp'.encode('utf-8'))
            logger.info("laptop -> microbit : 'badresp'")
            continue

        start_time = time()
        resp = readline(ser_micro)
        logger.info(f"microbit -> laptop : '{resp}'")

        if not resp.startswith('stop') and resp:
            ser_micro.write('badresp'.encode('utf-8'))
            logger.info("laptop -> microbit : 'badresp'")
            continue

        time_taken = time() - start_time
        submit(name, time_taken)




if __name__ == '__main__':
    main()
