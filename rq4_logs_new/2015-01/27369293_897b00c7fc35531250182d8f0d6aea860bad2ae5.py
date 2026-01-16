#!/usr/bin/python
# -*- coding: iso-8859-15 -*-

import sys
import SocketServer
import socket
import os
from xml.sax import make_parser
from xml.sax.handler import ContentHandler


class SmallSMILHandler(ContentHandler):

    def __init__(self):
        self.L_Primera = ['username', 'passwd']
        self.L_Segunda = ['ip', 'puerto']
        self.L_Tercera = ['puerto']
        self.L_Cuarta = ['ip', 'puerto']
        self.L_Quinta = ['path']
        self.L_Sexta = ['path']
        self.Lista = []

    def startElement(self, name, attrs):

        if name == 'account':
            dic = {"name": "acount"}
            for atributo in self.L_Primera:
                dic[atributo] = attrs.get(atributo, "")
            self.Lista.append(dic)
        elif name == 'uaserver':
            dic = {"name": "uaserver"}
            for atributo in self.L_Segunda:
                dic[atributo] = attrs.get(atributo, "")
            self.Lista.append(dic)
        elif name == 'rtpaudio':
            dic = {"name": "rtpaudio"}
            for atributo in self.L_Tercera:
                dic[atributo] = attrs.get(atributo, "")
            self.Lista.append(dic)
        elif name == 'regproxy':
            dic = {"name": "regproxy"}
            for atributo in self.L_Cuarta:
                dic[atributo] = attrs.get(atributo, "")
            self.Lista.append(dic)
        elif name == 'log':
            dic = {"name": "log"}
            for atributo in self.L_Quinta:
                dic[atributo] = attrs.get(atributo, "")
            self.Lista.append(dic)
        elif name == 'audio':
            dic = {"name": "audio"}
            for atributo in self.L_Sexta:
                dic[atributo] = attrs.get(atributo, "")
            self.Lista.append(dic)


    def get_tags(self):
        return self.Lista


class EchoHandler(SocketServer.DatagramRequestHandler):
    """
    Echo server class
    """

    def handle(self):
        while 1:
            # Leyendo línea a línea lo que nos envía el proxy/User Agent
            line = self.rfile.read()
            # Si no hay más líneas salimos del bucle infinito
            if not line:
                break
            print line
            Linea = line.split('\r\n')
            Linea = Linea[0].split(' ')
            Metodo = Linea[0]
            Lista_Metodos = ["Invite", "Bye", "Ack"]
            UserName = Lista[0]['username']
            if len(Linea) != 3 or Linea[2] != 'SIP/2.0':
                Envio =  'SIP/2.0 400 Bad Request\r\n\r\n'
                self.wfile.write(Envio)
            else:
                if not Metodo in Lista_Metodos:
                    if line != '':
                        Envio = 'SIP/2.0 405 Method Not Allowed\r\n\r\n'
                        self.wfile.write(Envio)
                else:
                    if Metodo == 'Invite':
                        Info = line.split('\r\n\r\n')
                        SDP = Info[1]
                        SDP = SDP.split('\r\n')
                        SDP1 = SDP [1]
                        IP = SDP1.split(' ')
                        IP = IP[3]
                        SDP4 = SDP[4]
                        Puerto = SDP4.split(' ')
                        Puerto = Puerto[3]
                        Envio = 'SIP/2.0 100 Trying\r\n\r\n'
                        Envio += 'SIP/2.0 180 Ringing\r\n\r\n'
                        Envio += 'SIP/2.0 200 OK\r\n'
                        Envio += 'Content-Type: application/sdp\r\n\r\n'
                        Envio += 'v = 0\r\no = ' + UserName + ' ' + Lista[1]['ip']
                        Envio += '\r\ns = misesion\r\n' + 't = 0\r\n' + 'm = audio '
                        Envio += Lista[2]['puerto'] + ' RTP\r\n\r\n'
                        Ip_vacia = str(Lista[1]['ip'])
                        if not Ip_vacia:
                            Ip_vacia = '127.0.0.1'
                        print Envio
                        self.wfile.write(Envio)
                        print 'Enviado'
                    elif Metodo == 'Ack':
                        print 'RTP......'
                        #aEjecutar es un string con lo que se ejecuta en la shell
                        #Añado la IP del cliente en el envio de audio. Quito la 127.0.0.1
                        #aEjecutar = './mp32rtp -i ' + IP + ' -p ' + Puerto + '< '
                        #aEjecutar += Lista[5]['audio']
                        #print "Vamos a ejecutar", aEjecutar
                        #os.system('chmod 755 mp32rtp')
                        #os.system(aEjecutar)
                    elif Metodo == 'Bye':
                        Envio = 'SIP/2.0 200 OK\r\n\r\n'
                        print Envio
                        self.wfile.write(Envio)



if __name__ == "__main__":
    # Creamos servidor de eco y escuchamos
    if len(sys.argv) == 2:
        if os.path.exists(sys.argv[1]):
            parser = make_parser()
            cHandler = SmallSMILHandler()
            parser.setContentHandler(cHandler)
            info_ua = sys.argv[1]
            parser.parse(open(info_ua))
            Lista = cHandler.get_tags()

            serv = SocketServer.UDPServer(("", int(Lista[1]['puerto'])), EchoHandler)
            print "Listening..."
            serv.serve_forever()
        else:
            print 'El fichero no existe'
    else:
        print 'Usage: python uaserver.py config'