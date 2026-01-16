import socket

def principal():

    mi_socket  = socket.socket()
    mi_socket.connect(('127.0.0.1', 9090))

    while True:
        mensaje = input(" mensaje a enviar => ")
        mi_socket.send(mensaje.encode())
        if (mensaje == 'cerrar'):
            break
        recibido = mi_socket.recv(1024)
        print("El servidor dice: " +recibido.decode())

    print("Adios")
    mi_socket.close()

if __name__=='__main__':
    principal()