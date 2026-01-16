"""This module represents python grep implementation with server connection"""

from argmanager import ArgManager
from connection import ServerConnection
from grepimpl import GrepImpl


def main():
    """
    Main function to fetch args, connect to server, find info in file and disconnects from server
    """
    try:
        ip_address, wildcard, numeric_id = ArgManager().passed_args
        c = ServerConnection(host=ip_address)

        GrepImpl(root_dir=c.cd,
                 wildcard=wildcard,
                 numeric_id=numeric_id).process()

        c.disconnect()

    except Exception as err:
        print("=== Error ===\n{}: {}".format(err.__class__, str(err)))


if __name__ == '__main__':
    main()