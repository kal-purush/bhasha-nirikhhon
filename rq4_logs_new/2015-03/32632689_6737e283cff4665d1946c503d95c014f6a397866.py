#!/usr/bin/python

import minimalmodbus
import serial
import time
import logging


class Sim900(object):
    def __init__ (self,port,baud=9600,bytesize=serial.EIGHTBITS, parity=serial.PARITY_NONE, stop=serial.STOPBITS_ONE, timeout=1):
        self.serialPort = serial.Serial(port,baud,bytesize,parity,stop,timeout)

    def sendAtCommand(self,command):
        self.serialPort.write(bytes(command+'\r\n',encoding='ascii'))
        self.status =  self.readCommandResponse()
        return self.status

    def readCommandResponse(self):
        time.sleep(0.25)
        while True:
            try:
                msg = self.serialPort.read(100).decode('ascii').strip()
            except Exception as e:
                print 'msg:'+ str(e)
            if msg:
                return msg

    def __del__(self):
        self.serialPort.close()



if __name__ == '__main__':
    try:
        """try:
            instrument = minimalmodbus.Instrument('/dev/ttyUSB0',1,mode='ASCII')
            instrument.serial.baudrate = 9600
            instrument.serial.bytesize = 7
            instrument.serial.parity = serial.PARITY_EVEN
            instrument.serial.stopbits = 1
            instrument.serial.timeout = 0.1
            instrument.mode = minimalmodbus.MODE_ASCII
        except Exception as e:
            print (e)"""
        try:
            modem = Sim900('/dev/ttyS0')
            try:
                modem.sendAtCommand('AT')
                print modem.status
            except Exception as e:
                print 'at:'+str(e)
            try:
                modem.sendAtCommand('AT+CREG?')
                print (modem.status)
            except Exception as e:
                print 'at+creg?:'+str(e)
            try:
   
                modem.sendAtCommand('AT+CGATT=1')
                print modem.status
            except Exception as e:
                print 'at+cgatt=1:'+str(e)
            modem.sendAtCommand('AT+CSTT="www"')
            print modem.status
            modem.sendAtCommand('AT+CIICR')
            print modem.status
            modem.sendAtCommand('AT+CIFSR')
            print (modem.status)
            modem.sendAtCommand('AT+CCLK')
            print modem.status
            
        except Exception as e:
            print('whole:'+str(e))
        level='1'
        while True:
            print'----------------------------------------------------'
            modem.sendAtCommand('AT+CIPSTART="UDP","52.74.38.213","50001"')
            print modem.status
            modem.sendAtCommand('AT+CIPSEND')
            """try:
                #level = instrument.read_register(4105)
                
            except Exception as e:
                print (e)"""
            #level='2'
            
            currentTime = time.strftime('%d/%m/%Y %H:%M:%S',time.gmtime())
            packet = 'DGD001;' + str(level) + ';' + str(currentTime)+'\x1A'
            print 'gmtime: ' + currentTime
            modem.sendAtCommand(packet)
            print modem.status
            modem.sendAtCommand('AT+CIPCLOSE')
            #time.sleep(1)
            print packet
            print '----------------------------------------------------'
            level = str(int(level)+1)

    except serial.SerialException as e:
        print e
        print '\nProgram aborted because there is error in opening the serial port.\n' 


