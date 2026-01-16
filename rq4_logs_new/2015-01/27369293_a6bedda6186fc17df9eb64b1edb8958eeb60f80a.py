#!/usr/bin/python
# -*- coding: iso-8859-15 -*-

import sys
import os
import socket
from xml.sax import make_parser
from xml.sax.handler import ContentHandler

fichero = sys.argv[1]


class SmallSMILHandler(ContentHandler):

    def __init__(self):
        self.L_Primera = ['username', 'passwd']
        self.L_Segunda = ['ip', 'puerto']
        self.L_Tercera = ['puerto']
        self.L_Cuarta = ['ip', 'puerto']
        self.L_Quinta = ['path']
        self.L_Sexta = ['path']
        self.Lista = []

    #Despues de haber creafdo una lista general, creado un diccionario con cada atributo
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



if __name__ == "__main__":
    # Creamos servidor de eco y escuchamos
    if len(sys.argv) == 4:
        if os.path.exists(sys.argv[1]):
            parser = make_parser()
            cHandler = SmallSMILHandler()
            parser.setContentHandler(cHandler)
            fichero = sys.argv[1]
            parser.parse(open(fichero))
            Lista = cHandler.get_tags()
            #no vaaaaaaaaaaaaaaaaaaaaaaaa
            Ip_vacia = str(Lista[3]['ip'])
            if not Ip_vacia:
                Ip_vacia = '127.0.0.1'
            print Lista

            PROXY = Lista[3]['ip']
            PORT_PX = int(Lista[3]['puerto'])
            # Creamos el socket del Proxy
            my_proxy = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
            my_proxy.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
            my_proxy.connect((PROXY, PORT_PX))
            
            UserName = Lista[0]['username']
            Metodo = sys.argv[2]
            Metodo = Metodo[0].upper() + Metodo[1:].lower()
            if Metodo == 'Register':
                Line = 'Register sip:' + UserName + ':'
                Line += Lista[1]['puerto'] + ' SIP/2.0\r\n' + 'Expires: '
                Line += sys.argv[3] + '\r\n\r\n'
            elif Metodo == 'Invite':
                Line = 'Invite sip:' + sys.argv[3] + ' SIP/2.0\r\n'
                Line += 'Content-Type: application/sdp\r\n\r\n'
                Line += 'v = 0\r\no = ' + UserName + ' ' + Lista[1]['ip']
                Line += '\r\ns = misesion\r\n' + 't = 0\r\n' + 'm = audio '
                Line += Lista[2]['puerto'] + ' RTP\r\n\r\n'
            elif Metodo == 'Bye':
                Line = 'Bye sip:' + sys.argv[3] + ' SIP/2.0\r\n\r\n'

            try:
                # Contenido que vamos a enviar
                my_proxy.send(Line)
                print 'Envio'
                data = my_proxy.recv(1024)
                print 'Enviamos...\r\n' + Line
                print 'Recibimos ' + data
            except socket.error:
                Texto = 'Error: No server listening at '
                Texto += PROXY + ' port ' + str(PORT_PX)
                print Texto
                raise SystemExit

            processed_data = data.split('\r\n')
            #Modifico la comprobación del mensaje del servidor y añado la escucha habilitando un buffer
            if processed_data[0] == "SIP/2.0 100 Trying" and\
            processed_data[2] == "SIP/2.0 180 Ringing" and\
            processed_data[4] == "SIP/2.0 200 OK":
                LINE = 'Ack sip:' + sys.argv[3] + ' SIP/2.0\r\n\r\n'
                print LINE
                my_proxy.send(LINE)
                print 'RTP.....'
                #aEjecutar es un string con lo que se ejecuta en la shell
                #Añado la IP del cliente en el envio de audio. Quito la 127.0.0.1
                #aEjecutar = './mp32rtp -i ' + IP + ' -p ' + Puerto + '< '
                #aEjecutar += Lista[5]['audio']
                #print "Vamos a ejecutar", aEjecutar
                #os.system('chmod 755 mp32rtp')
                #os.system(aEjecutar)
                #data = my_proxy.recv(1024)
            my_proxy.close()

        else:
            print 'El fichero no existe'
    else:
        print 'Usage: python uaclient.py config method option'