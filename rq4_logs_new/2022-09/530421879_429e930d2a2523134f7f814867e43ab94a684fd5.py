import cv2 as cv
import time
import numpy as np
from pathlib import Path
import os


def save_image(img_type, number, frame):
    img_path = Path('./data/') / img_type / (str(number).zfill(2) + '.jpg')
    img_path.parent.mkdir(parents=True, exist_ok=True)
    cv.imwrite(str(img_path), frame)
    print(img_path)


def run_capturing(img_type, no_phothos, current_no_images):
    vid = cv.VideoCapture(0)
    no_phothos += current_no_images
    img_counter = current_no_images
    while True:
        ret, frame = vid.read()
        if ret == True:
            cv.imshow(f'Press space to save image, press "q" to quit', frame)
                            
            key = cv.waitKey(1)
            if key % 256 == ord('q') or no_phothos <= img_counter:
                break

            if key % 256 == ord(' '):
                save_image(img_type, img_counter, frame)
                img_counter += 1
            
    vid.release()
    # Destroy all the windows
    cv.destroyAllWindows()


if __name__ == '__main__':
    img_type = input("Type of photo: ")
    how_many = int(input("How many phothos you want to create: "))
    current_no_images = len(os.listdir(os.getcwd() + f"/data/{img_type}")) if Path(os.getcwd() + f"/data/{img_type}").exists() else 0
    run_capturing(img_type, how_many, current_no_images)