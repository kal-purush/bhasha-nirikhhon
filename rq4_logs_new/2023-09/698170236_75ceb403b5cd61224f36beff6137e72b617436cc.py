import cv2 as cv
import numpy as np
from PyQt5 import QtWidgets
from PyQt5.QtCore import QTimer
from warning import WarningThread


# Load the pre-trained pose estimation model
net_pose = cv.dnn.readNetFromTensorflow('C:\\Users\\Gulsu\\Desktop\\human-pose-estimation-opencv-master\\graph_opt.pb')

# Load the video capture
videom = cv.VideoCapture('C:\\Users\\Gulsu\\Desktop\\Earthquake Classroom Video.mp4')
ret, frame1 = videom.read()
ret, frame2 = videom.read()

# Pose estimation parameters
inWidth = 368
inHeight = 368
thr_pose = 0.2

# Define the body parts and pairs for connecting them
BODY_PARTS = {
    "Nose": 0, "Neck": 1, "Right Shoulder": 2, "Right Elbow": 3, "Right Wrist": 4,
    "Left Shoulder": 5, "Left Elbow": 6, "Left Wrist": 7, "Right Hip": 8, "Right Knee": 9,
    "Right Ankle": 10, "Left Hip": 11, "Left Knee": 12, "Left Ankle": 13, "Chest": 14,
    "Background": 15
}

POSE_PAIRS = [
    ["Neck", "Right Shoulder"], ["Neck", "Left Shoulder"], ["Right Shoulder", "Right Elbow"],
    ["Right Elbow", "Right Wrist"], ["Left Shoulder", "Left Elbow"], ["Left Elbow", "Left Wrist"],
    ["Neck", "Right Hip"], ["Right Hip", "Right Knee"], ["Right Knee", "Right Ankle"],
    ["Neck", "Left Hip"], ["Left Hip", "Left Knee"], ["Left Knee", "Left Ankle"]
]


# İkinci kodunuzu bir işlev içinde tanımlayın
def show_ui():
    app = QtWidgets.QApplication([])
    Dialog = QtWidgets.QDialog()
    ui = Ui_Dialog()
    ui.setupUi(Dialog)
    Dialog.show()
    app.exec_()

# Deprem bildirimi için bir bayrak tanımlayın
deprem_bildirildi = False

while videom.isOpened():
    # Perform motion detection
    fark = cv.absdiff(frame1, frame2)
    gri = cv.cvtColor(fark, cv.COLOR_BGR2GRAY)
    blur = cv.GaussianBlur(gri, (5, 5), 0)
    _, esik = cv.threshold(blur, 80, 255, cv.THRESH_BINARY)
    genis = cv.dilate(esik, None, iterations=3)
    kontur, _ = cv.findContours(genis, cv.RETR_TREE, cv.CHAIN_APPROX_SIMPLE)

    for k in kontur:
        (x, y, w, h) = cv.boundingRect(k)
        if cv.contourArea(k) > 800:
            cv.rectangle(frame1, (x, y), (w + x, h + y), (0, 0, 255), 2)

    for k in kontur:
        if cv.contourArea(k) > 800:
            metin = 'ACIL DURUM! DEPREM OLUYOR...'
            metin_boyut = cv.getTextSize(metin, cv.FONT_HERSHEY_SIMPLEX, 1.5, 2)[0]
            x_metin = (frame1.shape[1] - metin_boyut[0]) // 2
            y_metin = (frame1.shape[0] + metin_boyut[1]) // 2
            cv.putText(frame1, metin, (x_metin, y_metin), cv.FONT_HERSHEY_SIMPLEX, 1.5, (0, 0, 255), 9)

            # Deprem algılandığında ikinci kodu çağırmak yerine uyarı penceresini başlatın
            if not deprem_bildirildi:
               warning_thread = WarningThread('''             
                                              12:00
                                 Friday, September 15, 2023 (GMT+3)
                  ArVis Technology Pendik/İstanbul konumunda anomali meydana geldi.
                                        Kişi sayısı : 40''')

               warning_thread.start()
            
               deprem_bildirildi = True


    # Perform pose estimation
    frameWidth = frame1.shape[1]
    frameHeight = frame1.shape[0]

    net_pose.setInput(cv.dnn.blobFromImage(frame1, 1.0, (inWidth, inHeight), (127.5, 127.5, 127.5), swapRB=True, crop=False))
    out = net_pose.forward()
    out = out[:, :19, :, :]

    assert(len(BODY_PARTS) <= out.shape[1])

    points = []

    for i in range(len(BODY_PARTS)):
        heatMap = out[0, i, :, :]
        _, conf, _, point = cv.minMaxLoc(heatMap)
        x = (frameWidth * point[0]) / out.shape[3]
        y = (frameHeight * point[1]) / out.shape[2]
        points.append((int(x), int(y)) if conf > thr_pose else None)

    for pair in POSE_PAIRS:
        partFrom = pair[0]
        partTo = pair[1]
        assert(partFrom in BODY_PARTS)
        assert(partTo in BODY_PARTS)

        idFrom = BODY_PARTS[partFrom]
        idTo = BODY_PARTS[partTo]

        if points[idFrom] and points[idTo]:
            cv.line(frame1, points[idFrom], points[idTo], (0, 255, 0), 3)
            cv.ellipse(frame1, points[idFrom], (3, 3), 0, 0, 360, (0, 0, 255), cv.FILLED)
            cv.ellipse(frame1, points[idTo], (3, 3), 0, 0, 360, (0, 0, 255), cv.FILLED)

    t, _ = net_pose.getPerfProfile()
    freq = cv.getTickFrequency() / 1000
    cv.putText(frame1, '%.2fms' % (t / freq), (10, 20), cv.FONT_HERSHEY_SIMPLEX, 0.5, (0, 0, 0))

    cv.imshow('Pose Estimation and Motion Detection', frame1)

    frame1 = frame2
    ret, frame2 = videom.read()

    if cv.waitKey(1) & 0xFF == ord('q'):
        break

videom.release()
cv.destroyAllWindows()
