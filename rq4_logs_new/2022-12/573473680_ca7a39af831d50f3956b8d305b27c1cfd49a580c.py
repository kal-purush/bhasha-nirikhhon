import cv2

# Load some pre-trained data on face frontals from opencv (haar cascade algorithm)
trained_face_data = cv2.CascadeClassifier('haarcascade_frontalface_default.xml')

# Choose an Image to detect faces in
img = cv2.imread('RDJ.jpg')
#img = cv2.imread('Image2.JPG')   # Try this by removing the hash

# Must convert to grayscale
grayscaled_img = cv2.cvtColor(img, cv2.COLOR_BGR2GRAY)

# Detect Faces
face_coordinates = trained_face_data.detectMultiScale(grayscaled_img)

# Draw rectangles around the faces For loop is used when multiple images are detected
for (x, y, w, h) in face_coordinates:
    cv2.rectangle(img, (x, y), (x+w, y+h), (0, 255, 0), 2)

#print(face_coordinates)

#
cv2.imshow("Aashish's Face Detector", img)

# Wait here in the code and listen for a key press
cv2.waitKey()

print("Code Completed")