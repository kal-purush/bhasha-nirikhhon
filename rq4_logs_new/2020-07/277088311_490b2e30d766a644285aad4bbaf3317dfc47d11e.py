 
import pytesseract
import  numpy as np
import cv2

pytesseract.pytesseract.tesseract_cmd='/usr/bin/tesseract'
try:
    image='test.jpg'
    print('Reading the',image)
    img = cv2.imread(image)
    img = cv2.resize(img, None, fx=1.5, fy=1.5, interpolation=cv2.INTER_CUBIC)
    kernel = np.ones((1, 1), np.uint8)
    img = cv2.dilate(img, kernel, iterations=1)
    img = cv2.erode(img, kernel, iterations=1)
    new_image = 'new' + '-' + image
    cv2.imwrite(new_image, img)
    read = pytesseract.image_to_string(new_image)
    print()
    print(read)

except Exception as e:
    print('\nplease provide proper name of the image')
    print(e)