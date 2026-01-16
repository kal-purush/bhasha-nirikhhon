import RPi.GPIO as GPIO
import time

GPIO.setmode(GPIO.BOARD)
GPIO.setwarnings(False)

def LEDGreen_On():
    GPIO.setup(22,GPIO.OUT)
    # set GPIO-a pin to HIGH
    GPIO.output(22,GPIO.HIGH)
    # pause for one second
    time.sleep(0.5)

def LEDGreen_Off():
    GPIO.setup(22,GPIO.OUT)
    # set GPIO-a pin to LOW
    GPIO.output(22,GPIO.LOW)
    # pause for one second
    time.sleep(0.5)

def LEDRed_On():
    GPIO.setup(24,GPIO.OUT)
    # set GPIO-a pin to LOW
    GPIO.output(24,GPIO.HIGH)
    # pause for one second
    time.sleep(0.5)

def LEDRed_Off():
    GPIO.setup(24,GPIO.OUT)
    # set GPIO-a pin to LOW
    GPIO.output(24,GPIO.LOW)
    # pause for one second
    time.sleep(0.5)

def LEDBlue_On():
    GPIO.setup(40,GPIO.OUT)
    # set GPIO-a pin to LOW
    GPIO.output(40,GPIO.HIGH)
    # pause for one second
    time.sleep(0.5)

def LEDBlue_Off():
    GPIO.setup(40,GPIO.OUT)
    # set GPIO-a pin to LOW
    GPIO.output(40,GPIO.LOW)
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
        lampu = input('Green, Red, or Blue: ')
        
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
            print('Selected: Red')
            kondisi = input('On or Off: ')
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
                LEDBlue_On()
                print('LED is on!')
                LEDGreen_Off()
                LEDRed_Off()
                LEDBlue_Off()
                print('LED is off!')

        if lampu == 'Blue':
            print('Selected: Blue')
            kondisi = input('On or Off: ')

            if kondisi == 'On' or kondisi == 'on':
                LEDBlue_On()
                print('LED is on!')
                time.sleep(1)
            
            elif kondisi == 'Off' or kondisi == 'off':
                LEDBlue_Off()
                print('LED is off!')
                time.sleep(1)

            else:
                print('Invalid command!')


        else:
            print('Invalid command!')