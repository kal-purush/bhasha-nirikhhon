from __future__ import print_function
import time
from sr.robot import *




a_th = 2.3
d_th = 0.4
gold_th=1.1
silver_th=1
R = Robot()

""" instance of the class Robot"""

def drive(speed, seconds):
    """
    Function for setting a linear velocity

    Args: speed (int): the speed of the wheels
	  seconds (int): the time interval
    """
    R.motors[0].m0.power = speed
    R.motors[0].m1.power = speed
    time.sleep(seconds)
    R.motors[0].m0.power = 0
    R.motors[0].m1.power = 0

def turn(speed, seconds):
    """
    Function for setting an angular velocity
    
    Args: speed (int): the speed of the wheels
	  seconds (int): the time interval
    """
    R.motors[0].m0.power = speed
    R.motors[0].m1.power = -speed
    time.sleep(seconds)
    R.motors[0].m0.power = 0
    R.motors[0].m1.power = 0
    

 #--- 140 degrees are taken for the front view so that the width is large enough to search the silver token and we do not miss it ---#  

def find_silver_token():
    dist=3
    for token in R.see():
        if token.dist < dist and token.info.marker_type is MARKER_TOKEN_SILVER and -70<token.rot_y<70:
            dist=token.dist
	    rot_y=token.rot_y
    if dist==3:
	return -1, -1
    else:
   	return dist, rot_y

#--- Since we have to avoid golden token and they are in the form of wall and not the obstacles therefore the width to search for golden token is shorter than silver token ---#

def find_golden_token():
    dist=100
    
    for token in R.see():
        if token.dist < dist and token.info.marker_type is MARKER_TOKEN_GOLD and -40<token.rot_y<40:
            dist=token.dist
	    rot_y=token.rot_y
    if dist==100:
	return -1, -1
    else:
   	return dist, rot_y
   	
#--- Getting the values of right side wall to make the robot run in a straight line and avoid colliding with the walls ---#
   	
def find_golden_token_right():

    dist=100
    for token in R.see():
        if token.dist < dist and token.info.marker_type is MARKER_TOKEN_GOLD and 70<token.rot_y<100:
            dist=token.dist
    if dist==100:
	return -1
    else:
   	return dist

#--- Getting the values of left side wall to make the robot run in a straight line and avoid colliding with the walls ---#

def find_golden_token_left():

    dist=100
    for token in R.see():
        if token.dist < dist and token.info.marker_type is MARKER_TOKEN_GOLD and -100<token.rot_y<-70:
            dist=token.dist
    if dist==100:
	return -1
    else:
   	return dist


def main():

	#Since we want the robot to run endlessly
	while 1:
		
		#--- Getting the information of Silver Token ---#
		dist_silver, rot_silver = find_silver_token()
		
		#--- Getting the information of Golden Token/Walls ---#
		dist_gold, rot_gold =find_golden_token()
		left_dist=find_golden_token_left()
		right_dist=find_golden_token_right()
		
		#--- ---#
		if (dist_gold>gold_th and dist_silver>silver_th) or (dist_gold>gold_th and dist_silver==-1):
			drive(130,0.1)


		if dist_silver<silver_th and dist_silver!=-1:
			
			if dist_silver < d_th:
				print("Got it!")
				if R.grab():
	    				print("Gotcha!")
				    	turn(30, 2)
				    	drive(20, 0.9)
				    	R.release()
				    	drive(-20,0.9)
					turn(-30,2)

	    		elif -a_th<=rot_silver<=a_th:
	    			drive(40, 0.1)
	    			print("Ah, that'll do.")
		    	elif rot_silver < -a_th:
				print("Left a bit...")
				turn(-10, 0.1)
			elif rot_silver > a_th:
				print("Right a bit...")
				turn(10, 0.1)

		#--- ---#
		if dist_gold<gold_th and dist_gold!=-1:
		
			print("Wall is close")
			
			if left_dist>right_dist:
				turn(-35, 0.1)
				print("Wall on the right "+ str(right_dist)+ ", the distance on the left is: "+str(left_dist))		
			elif right_dist>left_dist:
				turn(35, 0.1)
				print("Wall on the left "+ str(left_dist)+ ", the distance on the right is: "+str(right_dist))

main()






















	