#Functions
#
#   get_tesseract_version	Returns the Tesseract version installed in the system.
#   image_to_string 		Returns the result of a Tesseract OCR run on the image to string
#   image_to_boxes 		Returns result containing recognized characters and their box boundaries
#   image_to_data 		Returns result containing box boundaries, confidences, and other information.
#				Requires Tesseract 3.05+. For more information, please check the Tesseract TSV
#       	                documentation
#   image_to_osd 		Returns result containing information about orientation and script detection.

#Parameters
#
#   image_to_data(image, lang=None, config='', nice=0, output_type=Output.STRING)
#   image:	Object, PIL Image/NumPy array of the image to be processed by Tesseract
#   lang:	String, Tesseract language code string 
#   config:	String, Any additional configurations as a string, ex: config='--psm 6'
#   nice:	Integer, modifies the processor priority for the Tesseract run. Not supported on Windows.
#		Nice adjusts the niceness of unix-like processes.
#   output_type:Class attribute, specifies the type of the output, defaults to string. For the full
#		list of all supported types, please check the definition of pytesseract.Output class.

#!/usr/bin/env python
#!/usr/bin/python

from pytesseract import image_to_string
from PIL import Image
import pytesseract
import requests
import base64
import json
import cv2
import numpy as np
from pprint import pprint

import sys
if sys.version_info[0] >= 3:
    import PySimpleGUI as sg
else:
    import PySimpleGUI27 as sg
import os
from PIL import Image, ImageTk
import io


img = sg.PopupGetFile('Image file to select', default_path = '')

if not img:
    sg.PopupCancel('Canceling')
    raise SystemExit()

print(img)



# French text image to string
#print(pytesseract.image_to_string(img, lang='fra'))

print("==============================================================")
print("image_to_string")
print("==============================================================")
print(pytesseract.image_to_string(img, lang = 'ara'))


print("==============================================================")
print("image_to_boxes")
print("==============================================================")
print(pytesseract.image_to_boxes(img))

#------------------------------------------------------------------------------
# use PIL to read data of one image
#------------------------------------------------------------------------------
def get_img_data(f, maxsize = (1200, 850), first = False):
    """Generate image data using PIL
    """
    img = Image.open(f)
    img.thumbnail(maxsize)
    if first:                     # tkinter is inactive the first time
        bio = io.BytesIO()
        img.save(bio, format = "PNG")
        del img
        return bio.getvalue()
    return ImageTk.PhotoImage(img)
#------------------------------------------------------------------------------

# create the form that also returns keyboard events
window = sg.Window('Image Browser', return_keyboard_events=True, location=(0, 0), use_default_focus=False)

# make these 2 elements outside the layout as we want to "update" them later
# initialize to the first file in the list
filename = os.path.join(img)
#print("# name of first file in list")
#print("filename")
#print(filename)

image_elem = sg.Image(data = get_img_data(filename, first = True))
#print("image_elem")
#print(image_elem)

filename_display_elem = sg.Text(filename, size=(80, 3))
#print("filename_display_elem")
#print(filename_display_elem)

# define layout, show and read the form
col = [[filename_display_elem],
          [image_elem]]

col_files = [[sg.ReadButton('Next', size=(8,2)),
              sg.ReadButton('Prev', size=(8,2)),]]

layout = [[sg.Column(col_files), sg.Column(col)]]

window.Layout(layout)          # Shows form on screen

# loop reading the user input and displaying image, filename
##i=0
while True:
    # read the form
    event, values = window.Read()
    ##print(event, values)
