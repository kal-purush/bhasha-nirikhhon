import cv2
import numpy as np
import matplotlib.pyplot as plt
from tensorflow.keras.models import Model
from tensorflow.keras.layers import GlobalMaxPooling2D, Dense
from tensorflow.keras.optimizers import Adam
from PIL import Image  

def plot_images(data):
    plt.figure(figsize=(10, 10))
    class_names = data.class_names
    for images, labels in data.take(1):
        for i in range(32):
            ax = plt.subplot(6, 6, i + 1)
            plt.imshow(images[i].numpy().astype("uint8"))
            plt.title(class_names[np.argmax(labels[i])])
            plt.axis("off")

def emotion_detection(model):
# prevents openCL usage and unnecessary logging messages
    cv2.ocl.setUseOpenCL(False)

    # dictionary which assigns each label an emotion (alphabetical order)
    emotion_dict = {0: "Angry", 1: "Disgusted", 2: "Fearful", 3: "Happy", 4: "Neutral", 5: "Sad", 6: "Surprised"}

    # start the webcam feed
    cap = cv2.VideoCapture(0)
    while True:
        # Find haar cascade to draw bounding box around face
        ret, frame = cap.read()
        if not ret:
            break
        facecasc = cv2.CascadeClassifier('haarcascade_frontalface_default.xml')
        # gray = cv2.cvtColor(frame, cv2.COLOR_BGR2GRAY)
        # faces = facecasc.detectMultiScale(gray,scaleFactor=1.3, minNeighbors=5)
        faces = facecasc.detectMultiScale(frame,scaleFactor=1.3, minNeighbors=5)
        for (x, y, w, h) in faces:
            cv2.rectangle(frame, (x, y-50), (x+w, y+h+10), (255, 0, 0), 2)
            roi_gray = frame[y:y + h, x:x + w]
            # cropped_img = np.expand_dims(np.expand_dims(cv2.resize(roi_gray, (48, 48)), -1), 0)
            cropped_img = np.expand_dims(cv2.resize(roi_gray, (48, 48)), 0)
            
            prediction = model.predict(cropped_img)
            # print(prediction)
            maxindex = int(np.argmax(prediction))
            t1 = np.round(np.float(prediction[0][maxindex])*100)
            text = emotion_dict[maxindex] + " " + str(t1) + '%'
            cv2.putText(frame, text, (x+20, y-60), cv2.FONT_HERSHEY_SIMPLEX, 1, (255, 0, 0), 2, cv2.LINE_AA)

        cv2.imshow('Video', cv2.resize(frame,(400,400),interpolation = cv2.INTER_CUBIC))
        if cv2.waitKey(1) & 0xFF == ord('q'):
            break

    cap.release()
    cv2.destroyAllWindows()
    
def detectImage(model, frame):
# prevents openCL usage and unnecessary logging messages

    # dictionary which assigns each label an emotion (alphabetical order)
    emotion_dict = {0: "Angry", 1: "Disgusted", 2: "Fearful", 3: "Happy", 4: "Neutral", 5: "Sad", 6: "Surprised"}

    facecasc = cv2.CascadeClassifier('haarcascade_frontalface_default.xml')
    # frame = cv2.cvtColor(frame, cv2.COLOR_BGR2RGB)
    # frame = cv2.cvtColor(frame, cv2.COLOR_BGR2RGB)
    faces = facecasc.detectMultiScale(frame,scaleFactor=1.3, minNeighbors=5)
    for (x, y, w, h) in faces:
        cv2.rectangle(frame, (x, y-50), (x+w, y+h+10), (255, 0, 0), 2)
        roi_gray = frame[y:y + h, x:x + w]
        # cropped_img = np.expand_dims(np.expand_dims(cv2.resize(roi_gray, (48, 48)), -1), 0)
        cropped_img = np.expand_dims(cv2.resize(roi_gray, (48, 48)), 0)

        prediction = model.predict(cropped_img)
        # print(prediction)
        maxindex = int(np.argmax(prediction))
        t1 = np.round(np.float(prediction[0][maxindex])*100)
        text = emotion_dict[maxindex] + " " + str(t1) + '%'
        cv2.putText(frame, text, (x+20, y-60), cv2.FONT_HERSHEY_SIMPLEX, 1, (255, 0, 0), 2, cv2.LINE_AA)
    
    return cv2.resize(frame, (400,400))
def get_model(model_arch):
    pretrained_model = model_arch(include_top=False,
                         pooling='none',
                         input_shape=(48, 48, 3),
                         weights='imagenet')
    pretrained_model.trainable = False
    x = GlobalMaxPooling2D()(pretrained_model.output)
    x = Dense(2048, activation='relu')(x)
    x = Dense(2048, activation='relu')(x)
    output = Dense(7, activation='softmax')(x)
    return Model(pretrained_model.input, output)  


def emotion(ind):
# prevents openCL usage and unnecessary logging messages
    cv2.ocl.setUseOpenCL(False)

    # dictionary which assigns each label an emotion (alphabetical order)
    emotion_dict = {0: "Angry", 1: "Disgusted", 2: "Fearful", 3: "Happy", 4: "Neutral", 5: "Sad", 6: "Surprised"}

    # start the webcam feed
    path = 'data/' + emotion_dict[ind]
    count = 0
    cap = cv2.VideoCapture(0)
    while True:
        # Find haar cascade to draw bounding box around face
        ret, frame = cap.read()
        if not ret:
            break
        # cv2.imwrite("tmp.jpg", frame)
        facecasc = cv2.CascadeClassifier('haarcascade_frontalface_default.xml')
        # gray = cv2.cvtColor(frame, cv2.COLOR_BGR2GRAY)
        # faces = facecasc.detectMultiScale(gray,scaleFactor=1.3, minNeighbors=5)
        faces = facecasc.detectMultiScale(frame,scaleFactor=1.3, minNeighbors=5)
        c = 0
        for (x, y, w, h) in faces:
            cv2.rectangle(frame, (x, y-50), (x+w, y+h+10), (255, 0, 0), 2)
            roi_gray = frame[y:y + h, x:x + w]
            cropped_img = np.expand_dims(np.expand_dims(cv2.resize(roi_gray, (48, 48)), -1), 0)
            # prediction = model.predict(cropped_img)
            # maxindex = int(np.argmax(prediction))
            s = "XframeN%dF%d.jpg" % (count,c)
            cv2.imwrite(path + s, roi_gray)
            c += 1
            maxindex = 0
            cv2.putText(frame, emotion_dict[maxindex], (x+20, y-60), cv2.FONT_HERSHEY_SIMPLEX, 1, (255, 255, 255), 2, cv2.LINE_AA)
            count += 1

        cv2.imshow('Video', cv2.resize(frame,(400,400),interpolation = cv2.INTER_CUBIC))
        if cv2.waitKey(1) & 0xFF == ord('q'):
            break

    cap.release()
    cv2.destroyAllWindows()