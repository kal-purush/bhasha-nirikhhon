import RPi.GPIO as GPIO
import time

GPIO.setmode(GPIO.BCM)
GPIO.setwarnings(False)

def LEDGreen_On():
    GPIO.setup(25,GPIO.OUT)
    # set GPIO-a pin to HIGH
    GPIO.output(25,GPIO.HIGH)
    # pause for one second
    time.sleep(0.5)

def LEDGreen_Off():
    GPIO.setup(25,GPIO.OUT)
    # set GPIO-a pin to LOW
    GPIO.output(25,GPIO.LOW)
    # pause for one second
    time.sleep(0.5)

def LEDRed_On():
    GPIO.setup(8,GPIO.OUT)
    # set GPIO-a pin to LOW
    GPIO.output(8,GPIO.HIGH)
    # pause for one second
    time.sleep(0.5)

def LEDRed_Off():
    GPIO.setup(8,GPIO.OUT)
    # set GPIO-a pin to LOW
    GPIO.output(8,GPIO.LOW)
    # pause for one second
    time.sleep(0.5)

'''
Note: 
GPIO.setup(a,GPIO.OUT)
GPIO.output(a,GPIO.LOW)
GPIO.output(a,GPIO.HIGH)
 a is the number of gpio, a = 8 means gpio8
'''
if __name__ == '__main__':
    while True:
        lampu = input('Green or Red: ')
        
        if lampu == 'Green':
            print('Selected: Green')
            time.sleep(1)

            kondisi = input('On or Off: ')

            if kondisi == 'On' or kondisi == 'on':
                LEDGreen_On()
                print('LED is on!')
                time.sleep(1)
            
            elif kondisi == 'Off' or kondisi == 'off':
                LEDGreen_Off()
                print('LED is off!')
                time.sleep(1)

            else:
                print('Invalid command!')

        elif lampu == 'Red':
            kondisi = input('On or Off: ')
            print('Selected: Red')
            time.sleep(1)

            if kondisi == 'On' or kondisi == 'on':
                LEDRed_On()
                print('LED is on!')
                time.sleep(1)
            
            elif kondisi == 'Off' or kondisi == 'off':
                LEDRed_Off()
                print('LED is off!')
                time.sleep(1)

            else:
                print('Invalid command!')

        elif lampu == 'secret':
            print('Selected: secret')
            for k in range(1,11):
                LEDGreen_On()
                LEDRed_On()
                print('LED is on!')
                LEDGreen_Off()
                LEDRed_Off()
                print('LED is off!')

        else:
            print('Invalid command!')