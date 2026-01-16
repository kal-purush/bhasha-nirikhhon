from pymodbus.client.sync import ModbusSerialClient
from pymodbus.payload import BinaryPayloadDecoder
from pymodbus.constants import Endian
from pymodbus.client.sync import ModbusTcpClient
from pymodbus.compat import iteritems
import struct
import paho.mqtt.client as mqtt 
import time
from tkinter import *

def main():
    root=Tk()
    topFrame = Frame(root)
    topFrame.pack()
    
    
    button1 = Button(topFrame, text="Send",command=mqttClient)
    button1.pack(side=LEFT)
    #button2 = Button(topFrame, text="Stop",command=Stop_)
    #button2.pack(side=LEFT)
    button3 = Button(topFrame, text="Exit",command=Exit_)
    button3.pack(side=LEFT)

    root.mainloop()    

def Exit_():
    exit()    

def mqttClient():
    host ="siamgreenergy.com"
    port = 8000
    client = mqtt.Client()
    print("Connecting to broker")
    client.connect(host)

    #n = int(input(("Refresh time(s): "))) # input time to refresh
    #print("Refresh every ", n," second.")

    funcC = connect()
    client.publish("wlegat/wl002",funcC)
    time.sleep(1)       # time to refresh
    client.connect(host)
    #while True: # loop for sending infomation.
     #   funcC = connect()
      #  client.publish("wlegat/wl002",funcC)
       # time.sleep(n)       # time to refresh
        #client.connect(host)

def connect():
    client = ModbusSerialClient(
        method='rtu',
        port='COM3',
        baudrate=9600,
        timeout=3,
        parity='N',
        stopbits=1,
        bytesize=8
    )
    if client.connect():
        #ad1=int(input("address1: ")) # Address input normal
        x = 0
        list_A = []
        while(x<8):
            if(x == 0): #1 Voltage
                try:
                    ad1 = 1
                    result1 = client.read_input_registers(address=ad1-1,count=1,unit=1)  #Uint32/1
                    result2 = client.read_input_registers(address=ad1,count=1,unit=1)   #Uint32/2
        
                    r = result2.registers + result1.registers #[Uint32/2, Uint32/1]
                except AttributeError:
                    connect()
                print(r)
                b=struct.pack('HH',r[0],r[1]) 
                ans=struct.unpack('f',b)[0]
                ans = '%.2f'%ans
                
                list_A.append(ans) 

            if(x == 1): #7 Current
                try:
                    ad1 = 7
                    result1 = client.read_input_registers(address=ad1-1,count=1,unit=1)  #Uint32/1
                    result2 = client.read_input_registers(address=ad1,count=1,unit=1)   #Uint32/2
        
                    r = result2.registers + result1.registers #[Uint32/2, Uint32/1]
                except AttributeError:
                    connect()
                print(r)
                b=struct.pack('HH',r[0],r[1]) 
                ans=struct.unpack('f',b)[0]
                ans = '%.2f'%ans
                list_A.append(ans)
            if(x == 2): #13 Active power
                try:
                    ad1 = 13
                    result1 = client.read_input_registers(address=ad1-1,count=1,unit=1)  #Uint32/1
                    result2 = client.read_input_registers(address=ad1,count=1,unit=1)   #Uint32/2
        
                    r = result2.registers + result1.registers #[Uint32/2, Uint32/1]
                except AttributeError:
                    connect()
                print(r)
                b=struct.pack('HH',r[0],r[1]) 
                ans=struct.unpack('f',b)[0]
                ans = '%.2f'%ans
                list_A.append(ans)
            if(x == 3): #31 Power factor
                try:
                    ad1 = 31
                    result1 = client.read_input_registers(address=ad1-1,count=1,unit=1)  #Uint32/1
                    result2 = client.read_input_registers(address=ad1,count=1,unit=1)   #Uint32/2
        
                    r = result2.registers + result1.registers #[Uint32/2, Uint32/1]
                except AttributeError:
                    connect()
                print(r)
                b=struct.pack('HH',r[0],r[1]) 
                ans=struct.unpack('f',b)[0]
                ans = '%.2f'%ans
                list_A.append(ans)
            if(x == 4): #71 Frequency
                try:
                    ad1 = 71
                    result1 = client.read_input_registers(address=ad1-1,count=1,unit=1)  #Uint32/1
                    result2 = client.read_input_registers(address=ad1,count=1,unit=1)   #Uint32/2
        
                    r = result2.registers + result1.registers #[Uint32/2, Uint32/1]
                except AttributeError:
                    connect()
                print(r)
                b=struct.pack('HH',r[0],r[1]) 
                ans=struct.unpack('f',b)[0]
                ans = '%.2f'%ans
                list_A.append(ans)
            if(x == 5): #73
                ad1 = 73
                result1 = client.read_input_registers(address=ad1-1,count=1,unit=1)  
                result2 = client.read_input_registers(address=ad1,count=1,unit=1)
        
                r = result2.registers + result1.registers #[Uint32/2, Uint32/1]
                print(r)
                b=struct.pack('HH',r[0],r[1]) 
                ans=struct.unpack('f',b)[0]
                ans = '%.2f'%ans
                list_A.append(ans)
            if(x == 6): #75 Import active energy
                try:
                    ad1 = 75
                    result1 = client.read_input_registers(address=ad1-1,count=1,unit=1)  #Uint32/1
                    result2 = client.read_input_registers(address=ad1,count=1,unit=1)   #Uint32/2
        
                    r = result2.registers + result1.registers #[Uint32/2, Uint32/1]
                except AttributeError:
                    connect()
                print(r)
                b=struct.pack('HH',r[0],r[1]) 
                ans=struct.unpack('f',b)[0]
                ans = '%.2f'%ans
                
                if(ans == '0.00'):
                    ans = 'nan'
                list_A.append(ans)
    
         
            if(x == 7): #343 Total active energy
                try:
                    ad1 = 343
                    result1 = client.read_input_registers(address=ad1-1,count=1,unit=1)  #Uint32/1
                    result2 = client.read_input_registers(address=ad1,count=1,unit=1)   #Uint32/2
        
                    r = result2.registers + result1.registers #[Uint32/2, Uint32/1]
                except AttributeError:
                    connect()
                print(r)
                b=struct.pack('HH',r[0],r[1]) 
                ans=struct.unpack('f',b)[0]
                ans = '%.2f'%ans
                list_A.append(ans)

            print('x = ',x)
            x+=1
          
        print(list_A)
        
        allans = 'V = '+str(list_A[0])+', '+'I = '+str(list_A[1])+', '+'W = '+str(list_A[2])+', '+'PF = '+str(list_A[3])+', '+'Hz = '+str(list_A[4])+', '+'IAE kWh = '+str(list_A[5])+', '+'EAE kWh = '+str(list_A[6])+', '+'TAE kWh = '+str(list_A[7])
        print('allans: ',allans)
        client.close()
        return allans
    
    else:
        print('Cannot connect to the Modbus Server/Slave')

main()
    





