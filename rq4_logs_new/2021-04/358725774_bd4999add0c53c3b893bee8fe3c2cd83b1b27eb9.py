import cv2

# Load haar cascade
haar_cascade = cv2.CascadeClassifier('haarcascade_frontalface_default.xml')

# Get test images
img = cv2.imread('test1.jpg')

# Convert into grayscale
gray = cv2.cvtColor(img, cv2.COLOR_BGR2GRAY)

# Detect
faces = haar_cascade.detectMultiScale(gray, 1.27, 5)

# Draw rectangle around the faces
for (x, y, w, h) in faces:
    #cv2.rectangle(img, (x, y), (x + w, y + h), (0, 255, 0), 1)
    roi = img[y:y + h, x:x + w]
    # applying a gaussian blur over this new rectangle area
    roi = cv2.GaussianBlur(roi, (23, 23), 30)
    # impose this blurred image on original image to get final image
    img[y:y + roi.shape[0], x:x + roi.shape[1]] = roi

# Display the output
cv2.imshow('img', img)
cv2.waitKey()