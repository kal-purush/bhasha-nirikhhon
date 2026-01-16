#!/usr/bin/env python

# Licensed to the Apache Software Foundation (ASF) under one
# or more contributor license agreements.  See the NOTICE file
# distributed with this work for additional information
# regarding copyright ownership.  The ASF licenses this file
# to you under the Apache License, Version 2.0 (the
# "License"); you may not use this file except in compliance
# with the License.  You may obtain a copy of the License at
#
#     http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

import sys
import threading
import time
import os
import urllib2
import numpy as np
import cv2

import mesos.interface
from mesos.interface import mesos_pb2
import mesos.native


class MyExecutor(mesos.interface.Executor):
    def launchTask(self, driver, task):
        # Create a thread to run the task. Tasks should always be run in new
        # threads or processes, rather than inside launchTask itself.
        def run_task():
            print "Running task %s" % task.task_id.value
            update = mesos_pb2.TaskStatus()
            update.task_id.value = task.task_id.value
            update.state = mesos_pb2.TASK_RUNNING
            update.data = 'data with a \0 byte'
            driver.sendStatusUpdate(update)

            # This is where one would perform the requested task.
            self.main_func()

            print "Sending status update..."
            update = mesos_pb2.TaskStatus()
            update.task_id.value = task.task_id.value
            update.state = mesos_pb2.TASK_FINISHED
            update.data = 'data with a \0 byte'
            driver.sendStatusUpdate(update)
            print "Sent status update"

        thread = threading.Thread(target=run_task)
        thread.start()

    def frameworkMessage(self, driver, message):
        # Send it back to the scheduler.
        driver.sendFrameworkMessage(message)

    def url_to_image(self, url):
        # download the image, convert it to a NumPy array, and then read
        # it into OpenCV format
        try:
            resp = urllib2.urlopen(url, timeout=5)
            image = np.asarray(bytearray(resp.read()), dtype="uint8")
            image = cv2.imdecode(image, cv2.IMREAD_COLOR)
            # return the image
            return image
        except:
            print "Error reading from URL"
            return False

    def write_cascade(self):
        try:
            url = "https://raw.githubusercontent.com/opencv/opencv/master/data/haarcascades/haarcascade_frontalface_default.xml"
            resp = urllib2.urlopen(url, timeout=5)
            face_cascade = resp.read()
            file = open("cascade.xml", 'w')
            file.write(face_cascade)
            file.close()
        except:
            print "Error reading cascade"

    def find_faces_img(self, url, name):
        image = self.url_to_image(url)
        if not os.path.exists("cascade.xml"):
            print "Writing cascade"
            self.write_cascade()
        if type(image) != bool:
            try:
                face_cascade = cv2.CascadeClassifier("cascade.xml")
                gray = cv2.cvtColor(image, cv2.COLOR_BGR2GRAY)
                faces = face_cascade.detectMultiScale(gray, 1.3, 5)
                for (x, y, w, h) in faces:
                    image = cv2.rectangle(image, (x, y), (x + w, y + h), (255, 0, 0), 2)
                cv2.imwrite(name, image)
                return True
            except:
                print "Error finding faces"
                return False
        else:
            return False

    def main_func(self):
        i = 0
        itr = 0
        no_images = 10
        print "Started"
        input_folder_name = "/home/vgg_face_dataset/files/"
        fileList = os.listdir(input_folder_name)
        output_folder_name = "/home/output/"

        if not os.path.exists(output_folder_name):
            os.makedirs(output_folder_name)
        for file_name in fileList:
            if i > no_images:
                break
            if itr > no_images:
                break
            itr += 1

            print "Reading file" + file_name
            with open(input_folder_name + file_name) as f:
                content = f.readlines()
            k = 0
            for line in content:
                k += 1
                if k > 3:
                    break

                link = str.split(line)
                if len(link) > 6:
                    print "Getting image from URL::" + link[1]
                if self.find_faces_img(link[1], output_folder_name + file_name + link[0] + ".jpg"):
                    i += 1
        print sys.path[0]


if __name__ == "__main__":
    print "Starting executor"
    driver = mesos.native.MesosExecutorDriver(MyExecutor())
    sys.exit(0 if driver.run() == mesos_pb2.DRIVER_STOPPED else 1)