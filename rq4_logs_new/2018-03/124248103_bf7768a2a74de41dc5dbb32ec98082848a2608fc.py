"""Detects Emerald Ash Borers from a given image.

Can be run standalone, or imported into another script. To run as a script,

    python iotrees/detect.py --image examples/EAB-on-purple-trap.jpg

Otherwise, import into a script with

    from iotrees.detect import detect

"""

import argparse

import numpy as np
import cv2


def detect(filename: str, save_debug_images=False):
    # Upper and lower rgb space for selecting background
    # Don't be fooled by the name. Started gray, ended up everything but
    gray_lower = np.array([0, 0, 10], dtype='uint8')
    gray_upper = np.array([125, 168, 125], dtype='uint8')

    frame = cv2.imread(filename)

    if frame is None:
        print(f"Couldn't load {filename}!")
        return

    frame_boxes = frame.copy()

    # Again, don't be fooled by the variable name.
    # Not whitish as much as it selected out the background
    whitish = cv2.inRange(frame, gray_lower, gray_upper)
    whitish = cv2.GaussianBlur(whitish, (5, 5), 1)

    # Now flip the background to be white, subject dark
    opposite = cv2.bitwise_not(whitish)

    # Begin edge detection
    canny = cv2.Canny(opposite, 30, 150)

    # Find the contours
    _, contours, _ = cv2.findContours(canny.copy(), cv2.RETR_EXTERNAL, cv2.CHAIN_APPROX_SIMPLE)

    bug_count = 0

    # Count what we think are EABs by the number of corners
    for index, contour in enumerate(contours):
        perimeter = cv2.arcLength(contour, True)
        approx = cv2.approxPolyDP(contour, 0.02 * perimeter, True)

        if len(approx) < 10:
            perimeter = cv2.arcLength(contour, True)
            if perimeter > 150:
                bug_count += 1

                if save_debug_images:
                    rect = np.int32(cv2.boxPoints(cv2.minAreaRect(contour)))
                    cv2.drawContours(frame_boxes, [rect], -1, (0, 255, 0), 2)

    # Optional stuff for showing what was counted as EAB
    if save_debug_images:
        cv2.imwrite(f'debug/original.png', frame)
        cv2.imwrite(f'debug/whitish.png', whitish)
        cv2.imwrite(f'debug/opposite.png', opposite)
        cv2.imwrite(f'debug/canny.png', canny)
        cv2.imwrite(f'debug/boxes.png', frame_boxes)

    return bug_count


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument('-i', '--image', required=True, help='Path to image')
    ap.add_argument('-d', '--debug-images', default=False, action='store_true',
                    help='If True, debug images will be saved to debug/')

    args = ap.parse_args()

    num_bugs = detect(args.image, args.debug_images)

    print(f'Detected {num_bugs} bugs.')


if __name__ == '__main__':
    main()