# python CalculateLBP.py -f train.txt -p params.txt
# http://scikit-image.org/docs/dev/auto_examples/plot_local_binary_pattern.html

import cv2
import os
from skimage.feature import local_binary_pattern
from scipy.stats import itemfreq
# from sklearn.preprocessing import normalize
# import cvutils
import csv
from matplotlib import pyplot as plt
from sklearn.externals import joblib
import numpy as np
import argparse

class CalculateLBP:
    def __init__(self, fileName, paramFile):
        self.trainedTxt = fileName
        self.paramTxt = paramFile
        self.trainDict = dict()
        self.lbpHistogram = []
        self.addrImg = []
        self.tagNo = []

    def readTrainData(self):
        with open(self.trainedTxt, "rb") as csvfile:
            reader = csv.reader(csvfile, delimiter=';')

            for row in reader:
                self.trainDict[row[0]] = int(row[1])


    def calculateLBP(self):
        paramList = list()
        with open(self.paramTxt) as f:
            for line in f:
                paramList.append(int(line.strip()))
        print(paramList)
        for image in self.trainDict.iterkeys():
            print(image)
            img = cv2.imread(image)
            imgGray = cv2.cvtColor(img, cv2.COLOR_BGR2GRAY)

            # radius = 3
            # noPoints = 8 * radius
            radius = paramList[0]
            noPoints = paramList[1] * radius
            print(radius)
            print(noPoints)
            lbpImage = local_binary_pattern(imgGray, noPoints, radius, method='uniform')

            # Calculate the histogram
            x = itemfreq(lbpImage.ravel())
            # normalize the histogram
            hist = x[:, 1] / sum(x[:, 1])

            # hist = cv2.calcHist(lbp, [0], None, [256], [0, 256])
            # cv2.normalize(hist,hist)
            # hist = hist.flatten()

            self.addrImg.append(image)
            self.lbpHistogram.append(hist)
            self.tagNo.append(self.trainDict.get(image))
            joblib.dump((self.addrImg, self.lbpHistogram, self.tagNo), "lbp.pkl", compress=3)

    def showTrainingSet(self):
        nrows = 3
        ncols = 5
        fig, axes = plt.subplots(nrows, ncols)

        for row in range(nrows):
            for col in range(ncols):
                # print("*******")
                # print(addrImg[row * ncols + col])
                axes[row][col].imshow(cv2.cvtColor(cv2.imread(self.addrImg[row * ncols + col]), cv2.COLOR_BGR2RGB))
                # cv2.waitKey(0)
                axes[row][col].axis('off')
                axes[row][col].set_title("{}".format(self.trainDict.get(self.addrImg[row * ncols + col])))
                # axes[row][col].set_title("{}".format(os.path.split(addrImg[row*ncols+col])[1]))

        fig.canvas.draw()
        im_ts = np.fromstring(fig.canvas.tostring_rgb(), dtype=np.uint8, sep='')
        im_ts = im_ts.reshape(fig.canvas.get_width_height()[::-1] + (3,))
        cv2.imshow("Training Set", im_ts)
        cv2.waitKey()

if __name__ == "__main__":
    ap = argparse.ArgumentParser()
    ap.add_argument("-f", "--file", required=True, help="Path to the file that contains the indexs")
    ap.add_argument("-p", "--params", required=True, help="Path to store all the configurable variables")
    args = vars(ap.parse_args())
    params = args["params"]

    lbp = CalculateLBP(args["file"], params)
    lbp.readTrainData()
    lbp.calculateLBP()
    lbp.showTrainingSet()