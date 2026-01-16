import cv2
import sys

face_cascade = cv2.CascadeClassifier('haarcascade_frontalface_default.xml')
eye_cascade = cv2.CascadeClassifier('haarcascade_eye.xml')

# Draw a rectangle around the faces
def detect_face(faces, img, gray):
  for (x,y,w,h) in faces:
    cv2.rectangle(img,(x,y),(x+w,y+h),(255,0,0),2)
    roi_gray = gray[y:y+h, x:x+w]
    roi_color = img[y:y+h, x:x+w]

    # Draw a rectangle around the eyes
    eyes = eye_cascade.detectMultiScale(roi_gray, 2.3)
    for (ex,ey,ew,eh) in eyes:
        cv2.rectangle(roi_color,(ex,ey),(ex+ew,ey+eh),(0,255,0),2)

def main():
  image_path = sys.argv[1]

  # Read the image
  img = cv2.imread(image_path)
  gray = cv2.cvtColor(img, cv2.COLOR_BGR2GRAY)

  # Detection settings
  faces = face_cascade.detectMultiScale(
    gray,
    scaleFactor=1.1,
    minNeighbors=3,
    minSize=(30, 30)
  )
  print("Found {0} faces!".format(len(faces)))
  
  detect_face(faces, img, gray)

  cv2.imshow('img',img)
  cv2.waitKey(0)
  cv2.destroyAllWindows()
  
if __name__== "__main__":
  main()