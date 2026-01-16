import sys, math

# Auto-generated code below aims at helping you parse
# the standard input according to the problem statement.

R = int(raw_input()) # the length of the road before the gap.
G = int(raw_input()) # the length of the gap.
L = int(raw_input()) # the length of the landing platform.

# game loop
minSpeed = G + 1
while 1:
    S = int(raw_input()) # the motorbike's speed.
    X = int(raw_input()) # the position on the road of the motorbike.
    
    # Write an action using print
    # To debug: print >> sys.stderr, "Debug messages..."

    #print "SPEED" # A single line containing one of 4 keywords: SPEED, SLOW, JUMP, WAIT.
    
    if (X >= (R+G)) or (S > minSpeed): # isOnPlattform or isTooFast
        print "SLOW"
    elif (X+S) >= (R+G): # canReachPlattform then jump
        print "JUMP"
    elif S < minSpeed: # mustSpeedUp
        print "SPEED"
    else:
        print "WAIT"