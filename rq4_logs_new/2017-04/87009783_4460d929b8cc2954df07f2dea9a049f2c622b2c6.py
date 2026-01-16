#Bitmaptoarray.py
#Used for converting a bitmap picture to a javascript/python array
#Enter cropped image of what needs to be maintained, get array  back to use later on in bots.

#Using PIL
from PIL import Image
import numpy as np
import json

#Color Scheme
#INDEX : COLOR
# 0 : White
# 1 : Light Gray
# 2 : Gray
# 3 : Black
# 4 : pink
# 5 : Red
# 6 : Orange
# 7 : Brown
# 8 : Yellow
# 9 : Light Green
# 10 : Green
# 11 : Cyan
# 12 : Sea color?
# 13 : Blue
# 14 : Pink-Purple
# 15 : Purple

Color_List = [
    (255, 255, 255),
    (228, 228, 228),
    (136, 136, 136),
    (34, 34, 34),
    (255, 167, 209),
    (229, 0, 0),
    (229, 149, 0),
    (160, 106, 66),
    (229, 217, 0),
    (148, 224, 68),
    (2, 190, 1),
    (0, 211, 221),
    (0, 131, 199),
    (0, 0, 234),
    (207, 110, 228),
    (130, 0, 128)
    ]

# Specify image location to read later on
Image_Path = raw_input("Enter image path/location: ")
try:
    # Try to read image using pil and convert array values to RGB
    PIL_Image = Image.open(Image_Path).convert("RGB")
except:
    print "Something went wrong with opening the image, pls check your path again"
    exit()
# Get Width and height
Width, Height = PIL_Image.size
# Convert PIL object to pixel array
Pixel_Array = PIL_Image.load()
# Init a new numpy array to store color indices
Final_Array = np.zeros((Width, Height), dtype='int')

#Loop through image
for i in range(Width):
    for j in range(Height):
        #Get color of selected pixel
        color = Pixel_Array[i, j]
        try:
            #return the corresponding index in the newly created numpy array (on same position)
            Final_Array[i, j] = Color_List.index(color)
        except:
            #In case the color is different for some reason (Not an r/place image), Throw error
            print "Color value was not in list"
            exit()

# Save array for use in python
np.savetxt(Image_Path + ".Array",Final_Array, fmt='%i', delimiter='\t', header="Import in python using numpy.genfromtxt('PATH/FILENAME', skip_header=3 ,delimiter='\\t', dtype='int')\n\n")
#Save array for use in javascript (JSON)
with open(Image_Path + ".JSON", "w") as JSON_File:
    json.dump(Final_Array.tolist(), JSON_File)
#ALL DONE
print "Done!, Saved file!"