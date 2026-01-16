import serial
import time
bus = serial.Serial('/dev/ttyS0', 9600, timeout=1)
def enviar_comando(comando):
 ser.write(comando.encode())
try:
 while True:
 comando = input("Digite '1' para ligar o LED ou
'0' para desligar: ")
 if comando == '1' or comando == '0':
 enviar_comando(comando)
 else:
 print("Comando inválido. Digite '1' ou '0'.")
except KeyboardInterrupt:
 bus.close()
print("Comunicação encerrada")
