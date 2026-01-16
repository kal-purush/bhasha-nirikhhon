# import the necessary packages
from imutils import paths
from matplotlib import pyplot
import face_recognition
import argparse
import pickle
import cv2
import os

def rotate_image(image, degrees):
  # get image height, width
  (h, w) = image.shape[:2
                      ]
  # calculate the center of the image
  center = (w / 2, h / 2)
  scale = 1.0
  
  # Perform the counter clockwise rotation holding at the center
  M = cv2.getRotationMatrix2D(center, degrees, scale)
  rotated = cv2.warpAffine(image, M, (h, w))

  return rotated

def resize_image(image, minimum_size ):
  scale_percent = 50
  width = int(image.shape[1] / 2 )
  height = int(image.shape[0] / 2)
  while( width > minimum_size and height > minimum_size):
    width = int(width / 2 )
    height = int(height / 2)

  dim = (width, height)

  return cv2.resize(rgb, dim, interpolation = cv2.INTER_AREA)

def crop_face(image):
  ''' Receives a cv2 vector image and returns an image of a single face in it
  if none was found returns the original image '''

  resized = resize_image(image, 400)

  # detect the (x, y)-coordinates of the bounding boxes
  # corresponding to each face in the input image
  boxes = []
  degrees = 0
  while degrees < 360 and not boxes :
    # If none is found in a first try, it rotates the image counter clockwise
    # till it finds a face or has rotate it entirely
    degrees_rotation = 45
    resized = rotate_image(resized, degrees )
    boxes = face_recognition.face_locations(resized ,
      model=args["detection_method"])
    degrees = degrees + degrees_rotation
  
  if( boxes ) :
    bottom_y, bottom_x, floor_y, floor_x = boxes[0]
    return resized[ bottom_y:floor_y, floor_x:bottom_x ]
  return image


# MAIN PARA EXEMPLO E TESTES
# construct the argument parser and parse the arguments
ap = argparse.ArgumentParser()
ap.add_argument("-i", "--dataset", required=True,
  help="path to input directory of faces + images")
ap.add_argument("-e", "--encodings", required=True,
  help="path to serialized db of facial encodings")
ap.add_argument("-d", "--detection-method", type=str, default="cnn",
  help="face detection model to use: either `hog` or `cnn`")
args = vars(ap.parse_args())

# grab the paths to the input images in our dataset
print("[INFO] quantifying faces...")
imagePaths = list(paths.list_images(args["dataset"]))
 
# initialize the list of known encodings and known names
knownEncodings = []
knownNames = []


# loop over the image paths
for (i, imagePath) in enumerate(imagePaths):
  # extract the person name from the image path
  print("[INFO] processing image {}/{}".format(i + 1,
    len(imagePaths)))
  name = imagePath.split(os.path.sep)[-2]
 
  # load the input image and convert it from BGR (OpenCV ordering)
  # to dlib ordering (RGB)
  image = pyplot.imread(imagePath)
  rgb = cv2.cvtColor(image, cv2.COLOR_BGR2RGB)
  face = crop_face(rgb)
  if (face != []):
    cv2.imshow("Face", face)
    cv2.waitKey(0)