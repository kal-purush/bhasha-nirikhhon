import time
import datetime
import serial


#---SETUP---
ser = serial.Serial(
    port='/dev/ttyUSB0' ,
    baudrate = 9600,
    parity=serial.PARITY_NONE,
    stopbits=serial.STOPBITS_ONE,
    bytesize=serial.EIGHTBITS,
    timeout=1
    )
print ("Serial is open...");

print ("---SETUP DONE---");


def fnc_CommTransmit(msg):
    #print ("Transmitting.")
    serOut = (str(msg) + ";\n")
    ser.write(serOut)
    
def fnc_CommRecieve():
    
    Continue = True
    Errors=0
    
    try:
        #---READING SERIAL---
        state=ser.readline()
        #print(state)        
        #---GETTING 1st DATA(also reffered to as command/cmd)---
        str_state=str(state)
        #print ("State: " + str_state + " END")
        
        ###Splitting###
        state_split=str_state.split(";")
        str_data1 = str(state_split[0])
        #print("Split: " + str_data1)
        data = str_data1
        
        
        '''
        state_split=str_state.split("'")
        serial_cmd=state_split[0].split(" ")
        #print ("done, CMD:", serial_cmd[0])
        str_data1=str(serial_cmd[0])
        '''
        
        '''
TODO: LOG NOT WORKING
        #---LOG DATA1 IN cmdlog.txt---
        
        #get current time
        print("Generating Timestamp")
        timestamp= datetime.datetime.fromtimestamp(datetime.datetime.now().timestamp()).isoformat()
        print("Timestamp", timestamp)
        cmdlog = open("cmdlog.txt", "a")
        print("Opening file")
        str_cmdlog=(timestamp + " " + data + "; ")
        cmdlog.write(str_cmdlog)
        print("Writing to file")
        '''
        if len(data) > 1:
            return data
        

    except:
        #print("Dafuw")
        Errors = Errors + 1
        if Errors < 10:
            pass
        else:
            Continue = False
            #return "ERROR"
    