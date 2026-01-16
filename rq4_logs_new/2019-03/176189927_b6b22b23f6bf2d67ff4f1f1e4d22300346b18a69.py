import argparse
import time

import cv2

from src.helpers import pyramid_images
from src.helpers import slide_windows
from src.helpers import nms

from src.cat_classifier import CatClassifier

map = {
    0: "dog",
    1: "cat"
}

cat_classifier = CatClassifier('./model_weights/cat_model.pb')
ap = argparse.ArgumentParser()
ap.add_argument('--image', required=True)

args = vars(ap.parse_args())
(window_width, window_height) = (128, 128)

image = cv2.imread(args['image'])

predicts = []
scores = []
for (x, y, window) in slide_windows(image, 5, (window_width, window_height)):
    if window.shape[0] != window_height or window.shape[1] != window_width:
        continue

    labels, probs = cat_classifier.predict([window])
    label = labels[0]
    prob = probs[0]
    if label == 1:
        copy = image.copy()
        font = cv2.FONT_HERSHEY_SIMPLEX
        predicts.append([x, y, x + window_width, y + window_height])
        scores.append(prob)
        cv2.rectangle(copy,
                      (x, y),
                      (x + window_width, y + window_height),
                      (0, 255, 0),
                      2)
        cv2.putText(copy,
                    map[label],
                    (x + window_width - 10, y + window_height - 10),
                    font,
                    1,
                    (255, 255, 255),
                    1,
                    cv2.LINE_AA)
        cv2.imshow("window", copy)
        cv2.waitKey(1)
        time.sleep(0.035)

predicts_nms, _ = nms(predicts, scores, 0.4)

for box in predicts_nms:
    cv2.rectangle(image, (box[0], box[1]), (box[2], box[3]), (0, 255, 0), 2)
cv2.imwrite("./output.jpg", image)