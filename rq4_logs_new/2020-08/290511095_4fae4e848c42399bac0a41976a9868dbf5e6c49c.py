import cv2 #Needed for cropping and converting to mp4
import glob #Needed to get files for the mp4 conversion, could probably do without
import os #Needed to delete files
#Librarys i could probably replace with PYGAME:
import pyautogui #Needed for screenshots
import win32gui #Needed for getting mouse x,y
import msvcrt #Needed for button inputs
#Functions
def convert_key(_input):
    x = str(_input).split("'")
    return x[1]

def get_next_key():
    while True:
        if msvcrt.kbhit():
            key_stroke = msvcrt.getch()
            return key_stroke  

def zero_number(_input,_lenght):
    _input_lenght = len(str(_input))
    if _input_lenght == _lenght or _input_lenght > _lenght:
        return _input
    else:
        return ("0" * (_lenght - _input_lenght)) + str(_input)

def wait_until_key(_input):
    x = True
    while x:
        if get_next_key() == _input:
            x = False
#INIT
print("Set top left")
stop_key = get_next_key()  
top_left = win32gui.GetCursorPos()
print("Set bottom right")
wait_until_key(stop_key)
bottom_right = win32gui.GetCursorPos()
print("Recording... press '{}' to stop.".format(convert_key(stop_key)))

#RECORDING
record_now = True
fc = 0
while record_now:
    myScreenshot = pyautogui.screenshot()
    myScreenshot.save(r'frame_{}.jpg'.format(zero_number(fc,10)))
    fc += 1
    if msvcrt.kbhit() and stop_key == msvcrt.getch():
        record_now = False
        print("Stopped the recording.")

#RENDER VIDEO
print("Processing frames...")
img_array = []
for filename in glob.glob('*.jpg'):
    img = cv2.imread(filename)
    crop_img = img[top_left[1]:bottom_right[1],top_left[0]:bottom_right[0]]
    height, width, layers = crop_img.shape
    size = (width,height)
    img_array.append(crop_img)
print("Writing to file...") #OUTPUT TO FILE
out = cv2.VideoWriter('output.mp4',cv2.VideoWriter_fourcc(*'mp4v'), 15, size)
for i in range(len(img_array)):
    out.write(img_array[i])
out.release()
print("Removing frames...") #DELETE THE FRAMES
for filename in glob.glob('*.jpg'):
    os.remove(filename)
input("Done! Press any button to quit.")