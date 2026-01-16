import socket

HOST = '127.0.0.1'
PORT = 9999

def serve():
    connection = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    connection.bind((HOST,PORT))
    connection.listen(1)

    while 1:
        conn, addr = connection.accept()
        data = conn.recv(1024)
        print(data)
        conn.sendall("HTTP/1.1 200 OK\r\n"\
        "Server: python/3.5.2\r\n"\
        "Content-Type: text/html; charset=\"utf-8\"\r\n\r\n"\
        "<html></html>".encode('utf-8'))
        conn.close()

serve()