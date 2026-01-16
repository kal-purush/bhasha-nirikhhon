from flask import Flask,render_template,request,flash,redirect,url_for
from werkzeug.utils import secure_filename
import numpy as np
from deepface import DeepFace
import cv2
import os
import pickle
from tensorflow import keras
from PIL import Image


UPLOAD_FOLDER = 'static/'
ALLOWED_EXTENSIONS = set(['png', 'jpg', 'jpeg', 'gif'])
 
def allowed_file(filename):
    return '.' in filename and filename.rsplit('.', 1)[1].lower() in ALLOWED_EXTENSIONS

app = Flask(__name__)
app.secret_key = "secret key"
app.config['MAX_CONTENT_LENGTH'] = 16 * 1024 * 1024
app.config['UPLOAD_FOLDER'] = UPLOAD_FOLDER
@app.route('/')
def front_page():
    return render_template('frontpage.html')
@app.route('/prediction')
def predictionnn_page():
    return render_template('prediction.html')
@app.route('/dementia',methods=['GET','POST'])
def dementia_page():
    if request.method == 'GET':
        return render_template('dementia.html')
    else:
        age = float(request.form['age'])
        sex = float(request.form['sex'])
        educ = float(request.form['educ'])
        ses = float(request.form['ses'])
        mmse = float(request.form['mmse'])
        etiv = float(request.form['etiv'])
        nwbv = float(request.form['nwbv'])
        asf = float(request.form['asf'])
        
        rfc = pickle.load(open('./static/alzheimer_model.pkl','rb'))
        testing_data = np.array([[sex,age,educ,ses,mmse,etiv,nwbv,asf]])
        prediction = rfc.predict(testing_data)
        if prediction[0] == 1:
            senddata="The Chances of Getting Dementia disease is very high."
        else:
            senddata="The Chances of Getting Dementia disease is very less."

        return render_template('result.html',resultvalue=senddata)

@app.route('/image',methods=['GET','POST'])
def image_page():
    if request.method == 'GET':
        return render_template('image.html')
    else:
        if 'file' not in request.files:
            flash('No file part')
            return redirect(url_for('image_page'))
        
        file = request.files['file']
        if file.filename == '':
            flash('No image selected for uploading')
            return redirect(url_for('image_page'))

        if file and allowed_file(file.filename):
            
            filename = secure_filename(file.filename)
            fullname=os.path.join(app.config['UPLOAD_FOLDER'], filename)
            file.save(fullname)
            try:
                image = cv2.imread(fullname)
                result = DeepFace.analyze(image,actions=['emotion'])
                print(result)
                return render_template('resultimage.html',filename=filename,resultemotion=result['dominant_emotion'])
            except:
                flash('Please recheck image clarity and reupload again.')
                return redirect(url_for('image_page'))
        else:
            flash('Allowed image types are - png, jpg, jpeg, gif')
            return redirect(url_for('image_page'))

@app.route('/imagenl',methods=['GET','POST'])
def imagenl_page():
    if request.method == 'GET':
        return render_template('image1.html')
    else:
        if 'file' not in request.files:
            flash('No file part')
            return redirect(url_for('imagenl_page'))
        
        file = request.files['file']
        if file.filename == '':
            flash('No image selected for uploading')
            return redirect(url_for('imagenl_page'))

        if file and allowed_file(file.filename):
            
            filename = secure_filename(file.filename)
            fullname=os.path.join(app.config['UPLOAD_FOLDER'], filename)
            # file.save(fullname)
            try:
                model1 = keras.models.load_model("./static/my_model")
                img = Image.open(file).convert('L')
                x = np.array(img.resize((48,48)))
                x = x.reshape(1,48,48,1)
                res = model1.predict_on_batch(x)
                res=res[0]
                max_index=0
                max_score=0
                labels={0: 'angry', 1: 'disgust', 2: 'fear', 3: 'happy', 4: 'neutral', 5: 'sad', 6: 'surprise'}
                for i in range(len(res)):
                    if res[i]>max_score:
                        max_score=res[i]
                        max_index=i
                print(max_index)
                # image = cv2.imread(fullname)
                # result = DeepFace.analyze(image,actions=['emotion'])
                # print(result)
                return render_template('resultimage1.html',filename="",resultemotion=labels[max_index])
            except:
                flash('Please recheck image clarity and reupload again.')
                return redirect(url_for('imagenl_page'))
        else:
            flash('Allowed image types are - png, jpg, jpeg, gif')
            return redirect(url_for('imagenl_page'))

       
if __name__ == '__main__':
    app.run()