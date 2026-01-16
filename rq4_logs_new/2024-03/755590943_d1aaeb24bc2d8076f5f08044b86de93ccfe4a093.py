import cv2
import numpy as np

class yolo:

    def __init__(self, test_frame):
        self.net = cv2.dnn.readNet("skyguide/yolo/yolov3.weights", "skyguide/yolo/darknet/cfg/yolov3.cfg")
        layer_names = self.net.getLayerNames()
        self.output_layers = [layer_names[i - 1] for i in self.net.getUnconnectedOutLayers()]

        self.height, self.width, channels = test_frame.shape

    def box_frame(self, frame):
        outs = self._detect_objects(frame)
        class_ids, confidences, boxes = self._process_detections(frame, outs)
        frame = self._draw_boxes(frame, class_ids, confidences, boxes)
        return frame

    def _detect_objects(self, frame):
        blob = cv2.dnn.blobFromImage(frame, 0.00392, (64, 64), (0, 0, 0), True, crop=False)
        self.net.setInput(blob)
        outs = self.net.forward(self.output_layers)
        return outs

    def _process_detections(self, frame, outs):
        class_ids, confidences, boxes = [], [], []
        for out in outs:
            for detection in out:
                scores = detection[5:]
                class_id = np.argmax(scores)
                confidence = scores[class_id]
                if confidence > 0.5:
                    center_x, center_y = int(detection[0] * self.width), int(detection[1] * self.width)
                    w, h = int(detection[2] * self.width), int(detection[3] * self.width)
                    x, y = int(center_x - w / 2), int(center_y - h / 2)
                    boxes.append([x, y, w, h])
                    confidences.append(float(confidence))
                    class_ids.append(class_id)
        return class_ids, confidences, boxes

    def _draw_boxes(self, frame, class_ids, confidences, boxes):
        indexes = cv2.dnn.NMSBoxes(boxes, confidences, 0.3, 0.4)
        if len(indexes) > 0:
            for i in indexes.flatten():
                if class_ids[i] == 0:  # Assuming '0' is the class ID for people
                    x, y, w, h = boxes[i]
                    cv2.rectangle(frame, (x, y), (x + w, y + h), (255, 0, 0), 2)
        return frame