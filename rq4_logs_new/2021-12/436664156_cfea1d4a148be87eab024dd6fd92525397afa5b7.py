import picamera
import time
import Image
# from PIL import Image
from tesseract import image_to_string
# import pytesseract

def camera(str):
camera = picamera.PiCamera() 
camera.capture(str)  
camera.start_preview() 
camera.vflip = True 
camera.hflip = True 
camera.brightness = 60  

# print image_to_string(Image.open('test.png'))
print image_to_string(Image.open(str), lang='eng')
return image_to_string(Image.open(str), lang='eng')