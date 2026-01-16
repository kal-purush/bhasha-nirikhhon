from bluetooth import*
import RPi.GPIO as GPIO
import time

pir_sensor=4
GPIO.setmode(GPIO.BCM)
GPIO.setwarnings(False)
GPIO.setup(18, GPIO.OUT)
GPIO.setup(pir_sensor, GPIO.IN)

current_state=0
count_events=0



counter=0
data='Off'
light_status='Light Off!'
try:
    while True:
        time.sleep(0.1)
        current_state=GPIO.input(pir_sensor)
        if current_state==1:
            count_events+=1
            print("Motion detected: %s" % (count_events))
            
            
            if data=='On':
                
                print(data)
                GPIO.output(18, GPIO.LOW)
                light_status='Light off!'
                data='Off'
            else:
                GPIO.output(18, GPIO.HIGH)
                light_status='Light on!'
                data='On'
            print(light_status)
            time.sleep(1)
except KeyboardInterrupt:
    pass
finally:
    GPIO.cleanup()