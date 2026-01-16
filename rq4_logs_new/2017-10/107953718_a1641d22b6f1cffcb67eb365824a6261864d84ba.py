import math
import os
import time
import cv2
import dlib
import numpy as np
from multiprocessing import Process,Lock,Queue
from wide_resnet import WideResNet


class Buffer(object):
    """
    store the recent n frame's result:
    face center(x,y),age,gender
    """
    def __init__(self,n,w,h):
        self.n = n
        # width and height to search
        self.w = w
        self.h = h
        # n frame result
        self.bf = []
        self.query_cnt = 0
    
    def write(self,result):
        if len(self.bf) == self.n:
            self.bf.pop(0)
        self.bf.append(result)

    def query(self,x,y):
        # clear the buffer every 200 query
        if self.query_cnt == 200:
            self.query_cnt = 0
            self.bf = []
        else:
            self.query_cnt += 1

        # search around position (x,y)
        ages = []
        genders = []
        for frame in self.bf:
            for x_,y_,age,gender in frame:
                if x-self.w < x_ < x+self.w and y-self.h < y_ < y+self.h:
                    ages.append(age)
                    genders.append(gender)
        # return the search result
        if len(ages) == 0:
            return None,None
        else:
            age = sum(ages) / float(len(ages))
            gender = "M" if genders.count("M")>genders.count("F") else "F"
            return age,gender

class Person(object):
    """
    pid: the id for this person
    age: age for this person
    gender: gender for this person
    descriptor: a list of face descriptor for this person
    n: save at most n descriptor for this person
    query_count: this person has been query how many times
    """
    def __init__(self,pid,age,gender,descriptor,n=10):
        self.pid = pid
        self.age = age
        self.gender = gender
        self.descriptor = descriptor
        self.n = n
        self.query_count = 0
   
    # save at most n descriptor for this person 
    def update(self,descriptor):
        while len(self.descriptor) >= self.n:
            self.descriptor.pop(0)
        self.descriptor.append(descriptor)

    # calculate the distance for the query face_descriptor
    def compare(self,face_descriptor):
        distances = []
        for descriptor in self.descriptor:
            distances.append(self.calc_distance(descriptor,face_descriptor))
        distances.sort()
        return sum(distances[:3]) / len(distances[:3])

    def calc_distance(self,descriptor1,descriptor2):
        # calculate the Euclidean distance
        distance = 0.0
        for i in range(len(descriptor1)):
            distance += (descriptor1[i]-descriptor2[i])**2
        return math.sqrt(distance)


def init_database(path="./images/"):
    db = []
    # directly get pid,age,gender,descriptor from the txt file
    with open(path+'database.txt','r') as f:
        lines = f.readlines()
        for line in lines:
            pid,age,gender,descriptors = line.split(" ")
            pid,age = int(pid),int(age)
            descriptors = descriptors.split(";")
            descriptors = [tuple([float(i) for i in descriptor.split(",")]) for descriptor in descriptors]
            db.append(Person(pid,age,gender,descriptors))
    return db

def extract_descriptor(img,face_loc,sp,facerec):
    landmarks = sp(img, face_loc)
    return tuple(facerec.compute_face_descriptor(img, landmarks))

def save_image(path,img,face_loc,pid,age,gender):
    img_h, img_w, _ = np.shape(img)
    x1, y1, x2, y2, w, h = face_loc.left(),face_loc.top(),face_loc.right()+1,\
                           face_loc.bottom()+1,face_loc.width(),face_loc.height()
    xw1 = max(int(x1 - 0.4 * w), 0)
    yw1 = max(int(y1 - 0.4 * h), 0)
    xw2 = min(int(x2 + 0.4 * w), img_w - 1)
    yw2 = min(int(y2 + 0.4 * h), img_h - 1)
    face = img[yw1:yw2 + 1, xw1:xw2 + 1, :]

    if not os.path.exists(path+str(pid)):
        os.makedirs(path+str(pid))
    cv2.imwrite(path+str(pid)+"/"+str(age)+"_"+gender+"_"+str(int(time.time()))+".jpg",face)


def save_database(db_queue,path):
    # save the person descriptor when exit
    DB = db_queue.get()
    with open(path+'database.txt','w') as f:
        for person in DB:
            pid,age,gender,descriptors = person.pid,person.age,person.gender,person.descriptor
            descriptors = ";".join([",".join([str(i) for i in descriptor]) for descriptor in descriptors])
            line = " ".join([str(pid),str(age),gender,descriptors])
            f.writelines(line + "\n")

def update(db_queue,lock,sp,facerec,threshold,path,img,face_loc,age,gender):
    # get the database
    lock.acquire()
    DB = db_queue.get()
    db_queue.put(DB)
    lock.release()

    # extract descriptor
    face_descriptor = extract_descriptor(img,face_loc,sp,facerec)

    # conduct face identification, get the min distance person
    min_distance = 9999
    min_person = None
    for person in DB:
        distance = person.compare(face_descriptor)
        if distance < min_distance:
            min_distance = distance
            min_person = person
    # update the database
    lock.acquire()
    DB = db_queue.get()
    if min_distance < threshold:
        if gender!=min_person.gender or abs(age-min_person.age)>8:
            print "gender,age mismatch"
        else:
            print "it is in database,person id:{},distance: {}".format(min_person.pid,min_distance)
            for db_person in DB:
                if db_person.pid == min_person.pid:
                    db_person.query_count += 1
                    if db_person.query_count % 2 == 0:
                        db_person.update(face_descriptor)
                        save_image(path,img,face_loc,min_person.pid,age,gender)
    else:
        print "it is not in database,person id:{},distance:{}".format(len(DB),min_distance)
        DB.append(Person(len(DB),age,gender,[face_descriptor]))
        save_image(path,img,face_loc,len(DB),age,gender)
    db_queue.put(DB)
    lock.release()


def draw_label(image, point, label, font=cv2.FONT_HERSHEY_SIMPLEX,
               font_scale=1, thickness=2):
    color = (255,0,0) if "M" in label else (0,0,255)
    size = cv2.getTextSize(label, font, font_scale, thickness)[0]
    x, y = point
    cv2.rectangle(image, (x, y - size[1]), (x + size[0], y), color, cv2.FILLED)
    cv2.putText(image, label, point, font, font_scale, (255, 255, 255), thickness)

def draw_count(image, person_count, font=cv2.FONT_HERSHEY_SIMPLEX,
               font_scale=1, thickness=2):
    cv2.putText(image,str(person_count),(500,500),font,font_scale,(255,255,255),thickness )

def main():
    # path to save database
    path = "./images/"
    # face landmarks detection
    sp = dlib.shape_predictor("dlib_models/shape_predictor_5_face_landmarks.dat")
    # face descriptor extract model
    facerec = dlib.face_recognition_model_v1("dlib_models/dlib_face_recognition_resnet_model_v1.dat")
    # for face detection
    detector = dlib.get_frontal_face_detector()
    # threshold whether two faces are of the same person
    threshold = 0.55
    # multi process data communication,use queue and lock
    # or, python have share memory structure for multi processing?
    lock = Lock()
    db_queue = Queue()
    # process pool
    procs = []

    # load age&gender model
    depth = 16 # depth of network
    k = 8 # width of network
    weight_file = os.path.join("pretrained_models", "weights.18-4.06.hdf5")
    img_size = 64
    model = WideResNet(img_size, depth=depth, k=k)()
    model.load_weights(weight_file)

    #initialize database
    if not os.path.exists(path):
        os.makedirs(path)
        open(path+'database.txt', 'a').close()
        db_queue.put([])
    else:
        DB = init_database()
        db_queue.put(DB)

    # initialize buffer
    bf = Buffer(5,40,40)
    
    # capture video
    cap = cv2.VideoCapture(0)
    cap.set(cv2.CAP_PROP_FRAME_WIDTH, 4096)
    cap.set(cv2.CAP_PROP_FRAME_HEIGHT, 3072)
    
    skip_frame = 3
    cnt_frame = 0
    resize_scale = 2
    while True:
        # get video frame
        ret, origin_img = cap.read()
        img = cv2.resize(origin_img,(origin_img.shape[1]//resize_scale,origin_img.shape[0]//resize_scale))

        if not ret:
            print("error: failed to capture image")
            return -1

        if cnt_frame != skip_frame:
            cnt_frame += 1
            cv2.imshow("result", img)
            continue
        else:
            cnt_frame = 0
        
        input_img = cv2.cvtColor(img, cv2.COLOR_BGR2RGB)
        img_h, img_w, _ = np.shape(input_img)

        # detect faces using dlib detector
        detected = detector(input_img)
        if len(detected)==0:
            bf.write([])
        
        # for each face, get its age and gender
        faces_age = []
        faces_gender = []
        frame_result = []
        for i, d in enumerate(detected):
            x1, y1, x2, y2, w, h = d.left(), d.top(), d.right() + 1, d.bottom() + 1, d.width(), d.height()
            # query in buffer(results of last n frame)
            age, gender = bf.query((x1+x2)/2.0,(y1+y2)/2.0)
            if age is not None:
                print "it is in buffer,age:{},gender:{}".format(age,gender)
                faces_age.append(age)
                faces_gender.append(gender)
            else:
                # buffer query fail, predict age and gender
                xw1 = max(int(x1 - 0.4 * w), 0)
                yw1 = max(int(y1 - 0.4 * h), 0)
                xw2 = min(int(x2 + 0.4 * w), img_w - 1)
                yw2 = min(int(y2 + 0.4 * h), img_h - 1)
                face = np.empty((1, img_size, img_size, 3))
                face[0,:,:,:] = cv2.resize(img[yw1:yw2 + 1, xw1:xw2 + 1, :], (img_size, img_size))

                result = model.predict(face)
                ages = np.arange(0, 101).reshape(101, 1)
                age = int(result[1].dot(ages).flatten()[0])
                gender = "F" if result[0][0][0]>0.5 else "M"
                    
                faces_age.append(age)
                faces_gender.append(gender)
 
                # conduct face identification, update database
                proc = Process(target=update,args=(db_queue,lock,sp,facerec,threshold,path,img,d,age,gender))
                procs.append(proc)
                proc.start()

            frame_result.append([(x1+x2)/2.0,(y1+y2)/2.0,age,gender])

            if gender=="F": 
                cv2.rectangle(origin_img, (x1*resize_scale, y1*resize_scale), (x2*resize_scale, y2*resize_scale),(0,0,255), 2)
            else:
                cv2.rectangle(origin_img, (x1*resize_scale, y1*resize_scale), (x2*resize_scale, y2*resize_scale),(255,0,0), 2)

        # update buffer, save the result of this frame
        bf.write(frame_result)

        # person count
        lock.acquire()
        DB = db_queue.get()
        PID_COUNT = len(DB)
        db_queue.put(DB)
        lock.release()

        draw_count(origin_img,PID_COUNT)    
        
        # clear oldest process
        while len(procs) > 100:
            proc = procs.pop(0)
            proc.join()
            # proc.terminate()

        # draw results
        for i, d in enumerate(detected):
            label = "{}, {}".format(int(faces_age[i]),faces_gender[i])
            draw_label(origin_img, (d.left()*resize_scale, d.top()*resize_scale), label)
      
        cv2.imshow("result", origin_img)
        key = cv2.waitKey(30)

        if key == 27:
            for proc in procs:
                proc.join()
                # proc.terminate()
            save_database(db_queue,path)
            break


if __name__ == '__main__':
    main()