 #!/usr/bin/env python
import os
import RPi.GPIO as GPIO

GPIO.setwarnings(False)
GPIO.setmode(GPIO.BCM)
FAN_PIN = 23
GPIO.setup(FAN_PIN, GPIO.OUT)

                

#import turn_fan_off
def sensor():
    for i in os.listdir('/sys/bus/w1/devices'):
        if i !='w1_bus_master1':
            ds18b20 =i
    return ds18b20

def read(ds18b20):
    location = '/sys/bus/w1/devices/' + ds18b20 + '/w1_slave'
    tfile = open(location)
    text = tfile.read()
    tfile.close()
    secondline = text.split("\n")[1]
    temperaturedata = secondline.split(" ")[9]
    temperature = float(temperaturedata[2:])    
    celsius = temperature / 1000
    return celsius

def loop(ds18b20):
    while True:
       if read(ds18b20) != None:
        temp1= read(ds18b20)
        temp2 = 25
        file1 = open("Readings.txt","a")
        file1.write(str(temp1)+"\n")
        file1.close

       if temp1 <= temp2:
           # print "t2 is lesser"
            GPIO.output(FAN_PIN,False)            #import turn_fan_off  #keep the fan off

       elif temp1> temp2:
             # print "t1 is Greater"
              GPIO.output(FAN_PIN,True) # import turn_fan_on  #turn on the fan
 
       print "current temp %0.3f C" %  temp1
def kill():
    quit()


if __name__ == '__main__':
    try:
        serialNum = sensor()
        loop(serialNum) 
    except KeyboardInterrupt:
        kill()