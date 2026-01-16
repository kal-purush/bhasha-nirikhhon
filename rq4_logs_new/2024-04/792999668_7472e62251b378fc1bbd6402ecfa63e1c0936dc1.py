import cv2
import numpy as np

# Load the image
image = cv2.imread(r"C:\Users\Tnluser.PG038JDB\Downloads\test.jpg")

# Define the green screen color range (adjust these values as needed)
lower_green = np.array([0, 100, 0])
upper_green = np.array([100, 255, 100])

# Create a mask of the green screen area
mask = cv2.inRange(image, lower_green, upper_green)

# Invert the mask (to keep the green screen area)
mask = cv2.bitwise_not(mask)

# Apply the mask to the original image
result = cv2.bitwise_and(image, image, mask=mask)

# Save the result
cv2.imwrite(r"C:\Users\Tnluser.PG038JDB\Downloads\testnew.jpg", result)