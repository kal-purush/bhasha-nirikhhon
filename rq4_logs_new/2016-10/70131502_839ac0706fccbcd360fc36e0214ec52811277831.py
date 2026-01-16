
import cPickle
import copy
import random
import time

import lasagne
import theano
import theano.tensor as T
import numpy as np
import cv2
import os
import collections
import math

imgstack = []
labels = []

iterator = 1

descriptor = []
features = []

f = file('trained_model.save', 'rb')
model = cPickle.load(f)
f.close()



def intersection(r1, r2):
    """

    :param r1: Tuple of X1,Y1
    :param r2: Tuple of X2,Y2
    :return: (left,right,top,bottom) of new rectangle
    """
    left = max(r1[0], r2[0])
    right = min(r1[0] + 60, r2[0] + 60)
    top = max(r1[1], r2[1])
    bottom = min(r1[1] + 60, r2[1] + 60)

    return (left, right, top, bottom)


def main(testfile):
    # print("Loading data...")

    # print testfile
    # Prepare Theano variables for inputs and targets
    input_var = T.tensor4('inputs')
    target_var = T.ivector('targets')

    # Create a loss expression for validation/testing. The crucial difference
    # here is that we do a deterministic forward pass through the network,
    # disabling dropout layers.
    test_prediction = lasagne.layers.get_output(model, inputs=input_var,
                                                deterministic=True)
    test_loss = lasagne.objectives.categorical_crossentropy(test_prediction,
                                                            target_var)
    test_loss = test_loss.mean()
    # As a bonus, also create an expression for the classification accuracy:
    test_acc = T.mean(T.eq(T.argmax(test_prediction, axis=1), target_var),
                      dtype=theano.config.floatX)

    # Compile a second function computing the validation loss and accuracy:
    val_fn = theano.function([input_var, target_var], [test_prediction, test_loss, test_acc])

    threshold = 0.90

    start_time = time.time()

    image = cv2.imread("./test_images/{0}".format(testfile))
    out_image = np.copy(image)
    box = 60
    crawl = 20

    test(start_time,image,out_image,model,box,crawl,threshold)


def test(start_time, image, out_image,model,box,crawl,threshold):
    """

    :param start_time: Time prediction starts
    :param image: input image
    :param out_image: output image
    :param model: trained model
    :param box: size of the box in pixels for shift scanning
    :param crawl: size of the pixel scanning is done inside box
    :param threshold: threshold/accuracy of detection
    :return:
    """
    # Prepare Theano variables for inputs and targets
    input_var = T.tensor4('inputs')
    target_var = T.ivector('targets')

    # Create a loss expression for validation/testing. The crucial difference
    # here is that we do a deterministic forward pass through the network,
    # disabling dropout layers.
    test_prediction = lasagne.layers.get_output(model, inputs=input_var,
                                                deterministic=True)
    test_loss = lasagne.objectives.categorical_crossentropy(test_prediction,
                                                            target_var)
    test_loss = test_loss.mean()
    # As a bonus, also create an expression for the classification accuracy:
    test_acc = T.mean(T.eq(T.argmax(test_prediction, axis=1), target_var),
                      dtype=theano.config.floatX)

    # Compile a second function computing the validation loss and accuracy:
    val_fn = theano.function([input_var, target_var], [test_prediction, test_loss, test_acc])
    coord = {}
    print "scanning"
    for y in xrange(360, 720, crawl):
        for x in xrange(640, 1280, crawl):
            img = image[y:y + box, x:x + box]
            # cv2.imwrite("./lol/{2}{0}.{1}.jpg".format(x, y,filen), img)
            img = cv2.cvtColor(img, cv2.COLOR_RGB2GRAY)
            img = np.asarray(img, dtype='float64') / 255.
            img = cv2.resize(img, (60, 60))

            img_ = img.reshape(-1, 1, 60, 60)
            predict, err, acc = val_fn(img_, [1])
            lst = np.ndarray.tolist(predict[0])
            flag = lst.index(max(lst))

            if (flag is 1) and lst[flag] >= threshold:
                thres = lst[flag]

                if thres > 0.95:

                    coord.update({thres: (x, y)})
                    cv2.rectangle(out_image, (x, y), (x + box, y + box), (255, 0, 0), 2)
                    # cv2.imwrite("./qwer/{2}{0}{1}.jpg".format(x, y, filen),cv2.resize(img_bkp, (60, 60)))
                else:
                    cv2.rectangle(out_image, (x, y), (x + box, y + box), (0,255, 0), 2)

    """Uncomment below for intersection of top two matches"""
    # x = max(coord.iteritems())[1][0]
    # y = max(coord.iteritems())[1][1]
    # od = collections.OrderedDict(sorted(coord.items()))


    # detect_squares =  list(od.items())[-2:]
    # print detect_squares
    # final_coord=intersection(detect_squares[1][1],detect_squares[0][1])
    # print final_coord
    # print od,x,y
    # cv2.rectangle(out_image, (final_coord[0], final_coord[2]), (final_coord[1], final_coord[3]), (150, 0, 150), 2)
    # cv2.rectangle(out_image, (x, y), (x + box, y + box), (255, 0, 0), 2)

    print("Time to Predict", time.time() - start_time)
    cv2.imshow("Window", out_image)
    cv2.imwrite("ppt2.jpg",out_image) # uncomment to save a copy
    cv2.waitKey(0)
    time.sleep(0.025)

main("1.jpg")