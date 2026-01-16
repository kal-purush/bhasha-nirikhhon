import os
from pir_trigger import *
import time



def get_image_list():
	"""
	Retrieves the current list of images in the images/ directory
	:return: list of image filenames, relative to the images/ directory
	"""
	return [name for name in os.listdir('images') if
			os.path.isfile("images/" + name)]  # bugfix couunt not getting image count - don't trust stackoverflow!

def wait_for_image():
	# test
	print("running capture")
	# camera.capture("image"+self.images_num+".jpg")
	image_list = get_image_list()
	print(image_list)
	image_num = len(get_image_list()) + 1
	print(image_num)

	wait_capture("images/image" + str(image_num) + ".jpg")
	# my_gui.images_num += 1;
	#        detect_image("image.jpg",0.7,gui_callback)

while True:
	wait_for_image()
	time.sleep(0.05)