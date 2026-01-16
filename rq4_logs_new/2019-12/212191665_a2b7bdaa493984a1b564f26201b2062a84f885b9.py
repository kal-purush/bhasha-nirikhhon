import numpy as np
import RPi.GPIO as GPIO
from time import sleep

def basic_movement(turn_or_move, angle_or_distance):
    GPIO.setwarnings(False)
    GPIO.setmode(GPIO.BCM)

    ## Left motor
    GPIO.setup(22, GPIO.OUT)
    GPIO.setup(27, GPIO.OUT)
    # PWM pin
    GPIO.setup(13, GPIO.OUT)  
    pwm_left = GPIO.PWM(13, 100)
    
    ## Right motor
    GPIO.setup(23, GPIO.OUT)
    GPIO.setup(24, GPIO.OUT)
    # PWM pin
    GPIO.setup(12, GPIO.OUT)
    pwm_right = GPIO.PWM(12, 100)
    
    if (turn_or_move == "turn"):
        if (angle_or_distance < 0):
            # Determines direction of left motor
            GPIO.output(22, False)   #in1
            GPIO.output(27, True)    #in2
            pwm_left.start(100)      #enA
            # Determines direction of the right motor
            GPIO.output(23, True)    #in3
            GPIO.output(24, False)   #in4
            pwm_right.start(100)     #enB
        else:
            # Determines direction of left motor
            GPIO.output(22, True)
            GPIO.output(27, False)
            pwm_left.start(100)
            # Determines direction of the right motor
            GPIO.output(23, False)
            GPIO.output(24, True)
            pwm_right.start(100)

        # TODO: this will require some testing
        time = np.abs(angle_or_distance)/45
        sleep(time)
        
    elif (turn_or_move == "move"):
        if (angle_or_distance < 0):
            # Determines direction of left motor
            GPIO.output(22, True)
            GPIO.output(27, False)
            pwm_left.start(100)
            # Determines direction of the right motor
            GPIO.output(23, True)
            GPIO.output(24, False)
            pwm_right.start(100)
        else:
            # Determines direction of left motor
            GPIO.output(22, False)
            GPIO.output(27, True)
            pwm_left.start(100)
            # Determines direction of the right motor
            GPIO.output(23, False)
            GPIO.output(24, True)
            pwm_right.start(100)

        
        # TODO: this will require some testing
        time = np.abs(angle_or_distance)/2
        sleep(time)
        
    # Determines direction of left motor
    GPIO.output(22, False)
    GPIO.output(27, False)
    pwm_left.start(0)
    # Determines direction of the right motor
    GPIO.output(23, False)
    GPIO.output(24, False)
    pwm_right.start(0)

    pwm_left.stop()
    pwm_right.stop()
    