########################################
# EMR 2021 PROJEKT - SPRACH EREKENNUNG #
########################################

# Mitglieder:

#     Marcel Heinen
#     Sergey Rogachevsky
#     Yosua Kurniawan

# se_youbot-real_demo1.py Python 3.8.10 tested and works (18.08.2021) 

# Known Issues:
# time delay between Computer due to wireless connection (Real Robot when using wireless connectivity)
# awful sphinx audio accuracy
# sphinx audio could not recieve any numerical audio input

# Library and Packages:

# Speech Recognition:

#     $ pip install SpeechRecognition

# FOR WINDOWS USER NEED TO ADD:

#     $ pip install pipwin
#     $ pipwin install pyaudio

# FOR UBUNTU USER NEED TO ADD:

#     $ sudo apt-get install portaudio19-dev python3-pyaudio
#     $ pip install PyAudio

# pocketsphinx:

#     $ sudo apt install -y libgstreamer1.0-dev libgstreamer-plugins-base1.0-dev
#     $ sudo apt-get install swig3.0 or $ sudo apt-get install swig (only god knows)
#     $ sudo pip install pocketsphinx

# rospy:

#     $ sudo apt install python-rospy

# pyaudio:

#     $ sudo apt install python3-pyaudio

# In real youBot (the same as roscore terminal):

# roslaunch youbot_driver_ros_interface youbot_driver.launch

######################################################################

import speech_recognition as sr
from geometry_msgs.msg import Twist
from nav_msgs.msg import Odometry
import rospy
import time
import math

x=0
y=0
yaw=0


def init():
    global msg, velocity_publisher, pose_subscriber

    msg = Twist()
    msg.linear.x = 0
    msg.linear.y = 0
    msg.angular.x = 0
    msg.angular.y = 0

    try:
        
        rospy.init_node('se_youbot', anonymous=True)

        #declare velocity publisher
        cmd_vel_topic = "/cmd_vel"
        velocity_publisher = rospy.Publisher(cmd_vel_topic, Twist, queue_size=10)

        # #declare pose subscriber
        position_topic = "/odom"
        pose_subscriber = rospy.Subscriber(position_topic, Odometry, poseCallback) 

        time.sleep(2)
       
    except rospy.ROSInterruptException:
        rospy.loginfo("node terminated.")

    
def poseCallback(pose_message):
    global x
    global y, yaw
    x= pose_message.pose.pose.position.x
    y= pose_message.pose.pose.position.y

def get_audio():
    r = sr.Recognizer()
    with sr.Microphone() as source:
        audio = r.listen(source)
        text = ""
        try:
            # text = r.recognize_google(audio)
            text = r.recognize_sphinx(audio)
            print(text)
        except:
            print("Unrecognizeable")
    return text


def phrasing_audio_direction():
    text_lst_direction = ""
    direction = ""
    determine_direction = True

    while determine_direction: 
        print("Give direction input (forward, backward, left, right) :")
        text_lst_direction = get_audio()
        if "forward" in text_lst_direction:
            print("Moving Forward")
            direction = "forward"
            determine_direction = False
            return direction
        elif "backward" in text_lst_direction:
            print("Moving Backward")
            direction = "backward"
            determine_direction = False
            return direction
        elif "left" in text_lst_direction:
            print("Turning Left")
            direction = "left"
            determine_direction = False
            return direction
        elif "right" in text_lst_direction:
            print("Turning Right")
            direction = "right"
            determine_direction = False
            return direction  
        else:
            print("UNABLE TO RECOGNIZE COMMAND TRY AGAIN, error: 1")
            determine_direction = True

def phrasing_audio_distance():
    text_lst_distance = ""
    distance = ""
    determine_distance = True
    max_distance = 3 # declare max distance

    while determine_distance: 
        print("Give distance value (number):")
        text_lst_distance = input('Type a int value number 1 - 3') # use string input instead of audio input

        if isinstance(text_lst_distance, str) == True and str.isdigit(text_lst_distance) == False: # check if text_lst_distance recieve any string and if it can be converted to an int
            print("UNABLE TO RECOGNIZE COMMAND TRY AGAIN, error: 2")
            determine_distance = True

        elif  int(text_lst_distance) < max_distance: # text_lst_distance must recieve string value that is able to be converted to an int
            distance = text_lst_distance
            print(distance + " m")
            determine_distance = False
            return distance

        else:
            print("UNABLE TO RECOGNIZE COMMAND TRY AGAIN, error: 3")
            determine_distance = True

def move():
        # declare a Twist message to send velocity commands
        msg = Twist()
        # get current location 
        global x, y, speed
        x0=x
        y0=y
        speed = 0.1 # define the speed of turtle
        direction = phrasing_audio_direction()
        distance = phrasing_audio_distance()

        if direction == 'forward':
            msg.linear.x = abs(speed)
        elif direction == 'backward':
            msg.linear.x = -abs(speed)
        elif direction == 'left':
            msg.linear.y = abs(speed)
        elif direction == 'right':
            msg.linear.y = -abs(speed)    
            
        distance_moved = 0.0
        loop_rate = rospy.Rate(10) # we publish the velocity at 10 Hz (10 times a second)    
        cmd_vel_topic = '/cmd_vel'
        velocity_publisher = rospy.Publisher(cmd_vel_topic, Twist, queue_size=10)

        while True :
                rospy.loginfo("youBot move: " + direction +" for " + distance + " m")
                velocity_publisher.publish(msg)

                loop_rate.sleep()
                
                distance_moved = abs(0.5 * math.sqrt(((x-x0) ** 2) + ((y-y0) ** 2)))
                print(distance_moved)               
                if  not (distance_moved < int(distance)):
                    rospy.loginfo("reached")
                    break
        
        # finally, stop the robot when desiered distance reached
        msg.linear.x =0
        msg.linear.y =0
        velocity_publisher.publish(msg)

if __name__ == '__main__':

    init()
    move()
