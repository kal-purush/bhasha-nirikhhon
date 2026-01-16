from flask import Flask, render_template, request
import numpy as np
import cv2
from base64 import b64decode, b64encode
from cvzone.HandTrackingModule import HandDetector
from pyzbar.pyzbar import decode

app = Flask(__name__)

@app.route('/', methods=['GET','POST'])
def hello_world():
    request_type_str = request.method
    if request_type_str == 'GET':
        return render_template('index.html', user_image = '')
    else:

        return render_template('index.html', user_image = '')
    
@app.route("/api/checkanswer", methods=['GET','POST'])
def check_answer():
    txt64 = request.form.get("todo")
    encoded_data = txt64.split(',')[1]
    encoded_data = b64decode(encoded_data)
    nparr = np.frombuffer(encoded_data, np.uint8)
    img = cv2.imdecode(nparr, cv2.IMREAD_COLOR)
    crop_img = img[15:165, 45:195]
    webaddress =""
    decodedObjects = decode(crop_img)
    for obj in decodedObjects:
        webaddress=str(obj.data)
    
    if webaddress.find("http://answer:") >0:
        webaddress = webaddress[16:-1] # remove the first 16 and last character
        if webaddress=="cat":
            cv2.putText(crop_img, str("Correct"), (10, 10), cv2.FONT_HERSHEY_PLAIN, 1, (255, 0, 0), 1)
        else:
            cv2.putText(crop_img, str("Wrong"), (10, 10), cv2.FONT_HERSHEY_PLAIN, 1, (255, 0, 0), 1)
    
    _, im_arr = cv2.imencode('.png', crop_img)
    im_bytes = im_arr.tobytes()
    im_b64 = b64encode(im_bytes).decode("utf-8")
    return im_b64