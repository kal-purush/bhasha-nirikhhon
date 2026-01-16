import numpy as np
import argparse
import cv2

prototxt="MobileNetSSD_deploy.prototxt.txt"
model="MobileNetSSD_deploy.caffemodel"

# initialize the list of class labels MobileNet SSD was trained to
# detect, then generate a set of bounding box colors for each class
CLASSES = ["background", "aeroplane", "bicycle", "bird", "boat",
	"bottle", "bus", "car", "cat", "chair", "cow", "diningtable",
	"dog", "horse", "motorbike", "person", "pottedplant", "sheep",
	"sofa", "train", "tvmonitor"]
COLORS = np.random.uniform(0, 255, size=(len(CLASSES), 3))

# load our serialized model from disk
print("[INFO] loading model...")
net = cv2.dnn.readNetFromCaffe(prototxt, model)

# load the input image and construct an input blob for the image
# by resizing to a fixed 300x300 pixels and then normalizing it
# (note: normalization is done via the authors of the MobileNet SSD
# implementation)

def detect_image(image_path,confidence_min,callback):
	image = cv2.imread(image_path)
	(h, w) = image.shape[:2]
	blob = cv2.dnn.blobFromImage(cv2.resize(image, (300, 300)), 0.007843, (300, 300), 127.5)
	#blob = cv2.dnn.blobFromImage(cv2.resize(image, (300, 300)), 0.01, (300, 300), 127.5)

# pass the blob through the network and obtain the detections and
# predictions
	print("[INFO] computing object detections...")
	net.setPreferableBackend(cv2.dnn.DNN_BACKEND_OPENCV)
	net.setPreferableTarget(cv2.dnn.DNN_TARGET_CPU)
	net.setInput(blob)
	detections = net.forward()

# loop over the detections
	for i in np.arange(0, detections.shape[2]):
		confidence = detections[0, 0, i, 2]
		if confidence > confidence_min:
			idx = int(detections[0, 0, i, 1])
			callback(CLASSES[idx],confidence*100)
			label = "{}: {:.2f}%".format(CLASSES[idx], confidence * 100)
			print("[INFO] {}".format(label))
	return detections

def display_detections(image_path, detections,confidence_min, scale_factor=0.5):
	image_full = cv2.imread(image_path)
	image = cv2.resize(image_full, (0,0), fx=scale_factor, fy=scale_factor)
	(h, w) = image.shape[:2]
	for i in np.arange(0, detections.shape[2]):
        # extract the confidence (i.e., probability) associated with the
        # prediction
	        confidence = detections[0, 0, i, 2]

        # filter out weak detections by ensuring the `confidence` is
        # greater than the minimum confidence
        	if confidence > confidence_min:
                # extract the index of the class label from the `detections`,
                # then compute the (x, y)-coordinates of the bounding box for
                # the object
	                idx = int(detections[0, 0, i, 1])
	                box = detections[0, 0, i, 3:7] * np.array([w, h, w, h])
	                (startX, startY, endX, endY) = box.astype("int")

                # display the prediction
	                label = "{}: {:.2f}%".format(CLASSES[idx], confidence * 100)
	                print("[INFO] {}".format(label))
	                cv2.rectangle(image, (startX, startY), (endX, endY),
	                        COLORS[idx], 2)
	                y = startY - 15 if startY - 15 > 15 else startY + 15
	                cv2.putText(image, label, (startX, y),
	                        cv2.FONT_HERSHEY_SIMPLEX, 0.5, COLORS[idx], 2)
	cv2.imshow("Output", image)
	cv2.waitKey(0)
	cv2.destroyAllWindows()
#def test_callback(label, conf):
#	print("CB: "+label+" "+str(conf))

#detect_image("image.jpg",0.5, test_callback)