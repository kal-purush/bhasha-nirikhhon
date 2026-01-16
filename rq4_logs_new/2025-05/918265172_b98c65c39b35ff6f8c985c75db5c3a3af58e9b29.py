from socket import AF_INET, SOCK_STREAM, socket

def check_port(host, port):
    server = socket(AF_INET, SOCK_STREAM)
    try:
        server.bind((host, port))

        return True
    except OSError:
        return False
    finally:
        server.close()