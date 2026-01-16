import socket
import threading

client = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
client.connect(("127.0.0.1", 5555))

def receive_messages():
    while True:
        try:
            message = client.recv(1024).decode()
            if not message:
                break
            print(message)
        except:
            print("❌ Disconnected from server.")
            client.close()
            break

threading.Thread(target=receive_messages, daemon=True).start()

while True:
    message = input()
    if message.lower() == "exit":
        client.send(message.encode())
        client.close()
        break
    client.send(message.encode())