import socket
import json
import pickle



def send_ready(duck_is_ready, coord):
    json_file = json.dumps({"duck_is_ready" : duck_is_ready, "x" : coord[0], "y" : coord[1]})
    sock = socket.socket()
    sock.bind(('', 9090))
    sock.listen(1)
    conn, addr = sock.accept()
    print(pickle.dumps(json_file))
    print(pickle.loads((pickle.dumps(json_file))))
    conn.sendall(pickle.dumps(json_file))
    conn.close()

def main():
    duck_is_ready = True
    coord = [8, 4]
    send_ready(duck_is_ready, coord)

if __name__ == '__main__':
    main()