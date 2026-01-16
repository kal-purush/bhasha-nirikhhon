import cv2 as cv

img = cv.imread('fish.jpeg')
img = cv.resize(img, (img.shape[1]//2, img.shape[0]//2))
cv.imshow('FISH', img)

# the number of columns and rows is the "kernel size"
# there are several ways of blurring, this script aims to show all the different ways

# Average - taking the surrounding 8 pixels and averaging a value for the center pixel for every single pixel
average = cv.blur(img, (3,3))
cv.imshow('Average Blur', average)

# Gaussian blur - almost the same as the average but it gives each surrounding pixel a weight as well
# feels more natural than regular blurring
gauss = cv.GaussianBlur(img, (3,3), 0)
cv.imshow('GAUSS', gauss)

# Median Blur - takes the median of the surrounding pixels
# more effective in reducing noise, so its useful for advanced computer vision projects
median = cv.medianBlur(img, 3)
cv.imshow("MEDIAN", median)

# Bilateral Blurring
# most effective, used alot for computer vision projects
# because while it blurs the image, it also retains the edges in the image
bilateral = cv.bilateralFilter(img, 10, 35, 35)
cv.imshow('BILATERAL', bilateral)

cv.waitKey(0)
cv.destroyAllWindows()
exit(0)