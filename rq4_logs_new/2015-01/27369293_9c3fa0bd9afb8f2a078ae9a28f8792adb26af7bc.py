#!/usr/bin/python
# -*- coding: iso-8859-15 -*-

import sys
import SocketServer
import time
import os
import socket
from xml.sax import make_parser
from xml.sax.handler import ContentHandler


class SmallSMILHandler(ContentHandler):

    def __init__(self):
        self.L_Primera = ['name', 'ip', 'puerto']
        self.L_Segunda = ['path', 'passwdpath']
        self.L_Tercera = ['path']
        self.Lista = []

    def startElement(self, name, attrs):

        if name == 'server':
            dic = {"name": "server"}
            for atributo in self.L_Primera:
                dic[atributo] = attrs.get(atributo, "")
            self.Lista.append(dic)
        elif name == 'database':
            dic = {"name": "database"}
            for atributo in self.L_Segunda:
                dic[atributo] = attrs.get(atributo, "")
            self.Lista.append(dic)
        elif name == 'log':
            dic = {"name": "log"}
            for atributo in self.L_Tercera:
                dic[atributo] = attrs.get(atributo, "")
            self.Lista.append(dic)


    def get_tags(self):
        return self.Lista


class SIPRegisterHandler(SocketServer.DatagramRequestHandler):
    """
    Echo server class
    """

    def register2file(self):
        """ Escribe en el fichero los datos de los usuarios\
 con sus sesiones activas"""
        fichero = open('registered.txt', 'w')
        cadena = 'User' + '\t' + 'IP' + '\t' + 'Puerto' + '\t' + 'Hora actual'
        cadena += '\t' + 'Expires\r\n'
        for x in Usuarios.keys():
            cadena += x + '\t' + Usuarios[x][0] + '\t' + Usuarios[x][1] + '\t'
            cadena += str(Usuarios[x][2]) + '\t' + str(Usuarios[x][3]) + '\r\n'
        fichero.write(cadena)
        fichero.close()


    def handle(self):
        """Escribe dirección y puerto del cliente (de tupla client_address)"""
        self.client_address
        """ Comprueba si se ha caducado la sesion de algun usuario """
        for x in Usuarios.keys():
            tiempo_actual = time.time()
            if Usuarios[x][2] + Usuarios[x][3] < tiempo_actual:
                del Usuarios[x]

        while 1:
            # Leyendo línea a línea lo que nos envía el proxy/User Agent
            Recibido = self.rfile.read()
            # Si no hay más líneas salimos del bucle infinito
            if not Recibido:
                break
            Lista_Metodos = ["Register", "Invite", "Bye", "Ack"]
            print Recibido
            Linea = Recibido.split('\r\n')
            Linea = Linea[0].split(' ')
            Metodo = Linea[0]
            if len(Linea) != 3 or Linea[2] != 'SIP/2.0':
                Envio =  'SIP/2.0 400 Bad Request\r\n\r\n'
                self.wfile.write(Envio)
            else:
                if not Metodo in Lista_Metodos:
                    if line != '':
                        Envio = 'SIP/2.0 405 Method Not Allowed\r\n\r\n'
                        self.wfile.write(Envio)
                else:
                    if Metodo == 'Register':
                        Info = Linea[1].split(':')
                        Direc = Info[1]
                        IP = self.client_address[0]
                        Puerto = Info[2]
                        Expir = Recibido.split('\r\n')
                        Expir = Expir[1].split(' ')
                        Expir = Expir[1]
                        if Expir == '0':
                            if Direc in Usuarios:
                                del Usuarios[Direc]
                            elif not Direc in Usuarios:
                                Line = 'SIP/2.0 404 User Not Found'
                                self.wfile.write(Line)
                        elif Expir > '0':
                            if not Direc in Usuarios:
                                Hora = time.time()
                                Usuarios[Direc] = (self.client_address[0],\
         Puerto, time.time(), float(Expir))
                            Line = 'SIP/2.0 200 OK' + '\r\n\r\n'
                            self.wfile.write(Line)
                        self.register2file()
                    elif Metodo == 'Invite' or Metodo == 'Bye' or Metodo == 'Ack':
                        Direc = Recibido.split(' ')
                        Direc = Direc[1].split(':')
                        Direc = Direc[1]
                        if Direc in Usuarios:
                            #SACARLO DEL BUFFER Y ENVIAR AL OTRO UA
                            print '-------------Reenvio---------------'
                            PROXY = Usuarios[Direc][0]
                            PORT_PX = int(Usuarios[Direc][1])
                            # Creamos el socket del Proxy
                            my_socket = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
                            my_socket.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
                            my_socket.connect((PROXY, PORT_PX))
                            my_socket.send(Recibido)
                            print 'Enviado'
                            if Metodo == 'Invite' or Metodo == 'Bye':
                                Recibido = my_socket.recv(1024)
                                print Recibido
                                self.wfile.write(Recibido)
                            my_socket.close()
                        elif not Direc in Usuarios:
                            Line = 'SIP/2.0 404 User Not Found'
                            self.wfile.write(Line)



if __name__ == "__main__":
    # Creamos servidor de eco y escuchamos
    if len(sys.argv) == 2:
        if os.path.exists(sys.argv[1]):
            parser = make_parser()
            cHandler = SmallSMILHandler()
            parser.setContentHandler(cHandler)
            info_proxy = sys.argv[1]
            parser.parse(open(info_proxy))
            Lista = cHandler.get_tags()

            Usuarios = {}

            print 'Server MiServidorBigBang listening at port ' + Lista[0]['puerto']
            serv = SocketServer.UDPServer(("", int(Lista[0]['puerto'])), SIPRegisterHandler)
            serv.serve_forever()
        else:
            print 'El fichero no existe'
    else:
        print 'Usage: python proxy_registrar.py config'