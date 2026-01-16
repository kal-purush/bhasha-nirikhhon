import serial
import time
import datetime

#---SETUP---
ser = serial.Serial(
    port='/dev/serial0' ,
    baudrate = 9600,
    parity=serial.PARITY_NONE,
    stopbits=serial.STOPBITS_ONE,
    bytesize=serial.EIGHTBITS,
    timeout=1
    )
print ("Serial is open...");

print ("---SETUP DONE---");

int_breakErrorCount=0;
int_success=0;
int_failed=0;
int_total=0;
while True :
    try:
        #---READING SERIAL---
        state=ser.readline()
        snt = "Hello World \n"
        #print ("Reading");
        print(state)
        
        #---GETTING 1st DATA(also reffered to as command/cmd)---
        #print ("Converting and splitting");
        str_state=str(state)
        state_split=str_state.split("'")
        #print(str_state.split("'"))
        #print("done")
        #print ("get command")
        #print (state_split[1].split(" "))
        serial_cmd=state_split[1].split(" ")
        print ("done, CMD:", serial_cmd[0])
        str_data1=serial_cmd[0]
        
        #---LOG DATA1 IN cmdlog.txt---
        
        #get current time
        timestamp= datetime.datetime.fromtimestamp(datetime.datetime.now().timestamp()).isoformat()
        print("Timestamp", timestamp)
        cmdlog = open("cmdlog.txt", "a")
        print("Opening file")
        str_cmdlog=(timestamp + " " + str_data1 + "; ")
        cmdlog.write(str_cmdlog)
        print("Writing to file")
        
        #---LOG TO output.txt---
        
        #check if transmission was correct
        if str_data1 == "HelloWorld":
            print("Successfull transmission!")
            #log correct transmission
            
            #get current time
            timestamp= datetime.datetime.fromtimestamp(datetime.datetime.now().timestamp()).isoformat()
            print("Timestamp", timestamp)
            cmdlog = open("output.txt", "a")
            print("Opening file")
            int_success = int_success + 1
            int_total = int_total + 1
            str_success = str(int_success)
            str_failed = str(int_failed)
            str_total = str(int_total)
            str_cmdlog=(timestamp + " " + str_success + " Successfull, " + str_failed + " Failed " + str_total + " Total; ")
            cmdlog.write(str_cmdlog)
            print("Writing to file")
            
        else:
            print("Failed transmission!");
            #log failed transmission
            
            #get current time
            timestamp= datetime.datetime.fromtimestamp(datetime.datetime.now().timestamp()).isoformat()
            print("Timestamp", timestamp)
            cmdlog = open("output.txt", "a")
            print("Opening file")
            int_failed = int_failed + 1
            int_total = int_total + 1
            str_success = str(int_success)
            str_failed = str(int_failed)
            str_total = str(int_total)
            str_cmdlog=(timestamp + " " + str_success + " Successfull, " + str_failed + " Failed " + str_total + " Total; ")
            cmdlog.write(str_cmdlog)
            print("Writing to file")
        
        
    except:
        print ("ErrorInLoop");
        #get current time
        timestamp= datetime.datetime.fromtimestamp(datetime.datetime.now().timestamp()).isoformat()
        print("Timestamp", timestamp)
        cmdlog = open("output.txt", "a")
        print("Opening file")
        int_breakErrorCount = int_breakErrorCount + 1
        str_cmdlog=(timestamp + " " + int_breakErrorCount + " BreakErrors;")
        cmdlog.write(str_cmdlog)
        print("Writing to file")
        
        
        pass