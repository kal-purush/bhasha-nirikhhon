#!/bin/python
#Test code for RPi remote connection


#!/usr/bin/python3
import RPi.GPIO as GPIO
import time
from datetime import datetime
from datetime import datetime
import mysql.connector as mariadb


#Initialize connection
mariadb_connection = mariadb.connect(user='root', password='raspberry', database='distance')
cursor = mariadb_connection.cursor()
number = 1
try:
        f=open("test.txt","a+")
        while True:
                #Configure GPIO
                GPIO.setmode(GPIO.BOARD)
                PIN_TRIGGER = 7
                PIN_ECHO = 11
                GPIO.setup(PIN_TRIGGER, GPIO.OUT)
                GPIO.setup(PIN_ECHO, GPIO.IN)
                GPIO.output(PIN_TRIGGER, GPIO.LOW)

                #Sensor start-up
                print("Waiting for sensor to settle")
                time.sleep(2)

                print("Calculating distance")
                GPIO.output(PIN_TRIGGER, GPIO.HIGH)
                time.sleep(0.00001)
                GPIO.output(PIN_TRIGGER, GPIO.LOW)

                while GPIO.input(PIN_ECHO)==0:
                        pulse_start_time = time.time()
                while GPIO.input(PIN_ECHO)==1:
                        pulse_end_time = time.time()

                pulse_duration = pulse_end_time - pulse_start_time
                distance = round(pulse_duration * 17150, 2)
                czas = datetime.now().replace(microsecond=0).isoformat(' ')
                print("Date: " + czas)
                print("Distance: " + str(distance) + "cm")
                if number == 1:
                        print "Mean: ",distance,"\n"
                        mean = distance
                        number = number + 1
                elif number > 1:
                        mean = (float(number-1)/float(number))*mean+((1.0/number)*distance)
                        print "Mean: ",mean,"\n"
                        number = number + 1
                # TODO: Push mean to db
                # Push to db
                f.write("Distance: %d\r\n" % distance)
                cursor.execute("INSERT INTO Encoder_abs (abs,date) VALUES (%s, %s)", (distance,czas))
                mariadb_connection.commit()
                GPIO.cleanup()


finally:
        GPIO.cleanup()
        f.close()