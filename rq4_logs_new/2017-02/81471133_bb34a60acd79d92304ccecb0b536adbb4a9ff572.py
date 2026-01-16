import numpy as np
import cv2


################
#1) load and play video in slow motion
#2) Press key "spacebar and hold" when there is eye-contact
#5) record 1 when space bar is pressed and hold
#6) record 0 if no key is being pressed
#7) output csv file 0 or 1 for all seconds in series ( x= i+1, y= 0 or 1)
#################

import numpy as np
import cv2

cap = cv2.VideoCapture('03MC_Nabeelcropped.mpg')

current_state = False
annotation_list = []

while(True):
    # Read one frame.
    ret, frame = cap.read()
    if not ret:
        break

    # Show one frame.
    cv2.imshow('frame', frame)

    # Check, if the space bar is pressed to switch the mode.
    if cv2.waitKey(14) & 0xFF == ord(' '):
        current_state = not current_state

    annotation_list.append(current_state)

# Convert the list of boolean values to a list of int values.
annotation_list = map(int, annotation_list)
print annotation_list

cap.release()
cv2.destroyAllWindows()