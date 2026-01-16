import cv2 as cv
import numpy as np
import sys
import matplotlib.pyplot as plt

if __name__ == '__main__':
    print(__doc__)

    model = "/mnt/ssd/usman_ws/Edge-Boxes-Python-Docker/models/model.yml.gz"
    im = cv.imread("testimages/3.jpg")

    edge_detection = cv.ximgproc.createStructuredEdgeDetection(model)
    rgb_im = cv.cvtColor(im, cv.COLOR_BGR2RGB)
    edges = edge_detection.detectEdges(np.float32(rgb_im) / 255.0)

    orimap = edge_detection.computeOrientation(edges)
    edges = edge_detection.edgesNms(edges, orimap)

    edge_boxes = cv.ximgproc.createEdgeBoxes()
    edge_boxes.setMaxBoxes(15)
    boxes = edge_boxes.getBoundingBoxes(edges, orimap)

    for b in boxes[0]:
        x, y, w, h = b
        color1 = (list(np.random.choice(range(256), size=3)))  
        color =[int(color1[0]), int(color1[1]), int(color1[2])]  
        cv.rectangle(im, (x, y), (x+w, y+h), color, 1, cv.LINE_AA)

    # cv.imshow("edges", edges)
    # cv.imshow("edgeboxes", im)

    im1 = plt.imshow(im)
    plt.show()
    # cv.imshow("edges", edges)
    # cv.imshow("edgeboxes", im)
    # cv.waitKey(0)
    # cv.destroyAllWindows()