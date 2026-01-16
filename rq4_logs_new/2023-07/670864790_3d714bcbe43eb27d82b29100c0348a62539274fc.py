import socket
import fcntl
import struct
import utils.log_utils
from global_def import *

log = utils.log_utils.logging_init(__file__)


def get_ip_address(ifname):
    ip = None
    s = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
    # noinspection PyBroadException
    try:
        ip = socket.inet_ntoa(fcntl.ioctl(
            s.fileno(),
            0x8915,  # SIOCGIFADDR
            struct.pack('256s', ifname[:15].encode())
        )[20:24])
    except Exception as e:
        log.debug("%s", e)
        ip = None
    finally:
        return ip
