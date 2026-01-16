# importando o modulo socket
import socket

from nsip import *

# definindo o IP do servidor
IP = "127.0.0.1"

# definindo a porta do servidor
PORTANUMERO = 2102

# criando um socket Internet (INET IPv4) sobre TCP
s = socket.socket(socket.AF_INET, socket.SOCK_STREAM)

# conectando ao servidor
s.connect((IP, PORTANUMERO))

# enviando a requisicao
msg = "horaAtual".encode("ISO-8859-1")
s.send(msg)

# recebendo a hora do servidor
buffer = s.recv(500)

# imprimindo a hora recebida
print("Hora certa: %s" % buffer.decode("ISO-8859-1"))

# fechando o socket
s.close()