from flask import Flask, flash, request, redirect, url_for, render_template
import os
from werkzeug.utils import secure_filename
import numpy as np 
import pandas as pd
from keras.datasets import cifar10
from skimage.feature import hog
from skimage import data, exposure
from sklearn import svm
from sklearn.metrics import accuracy_score
from PIL import Image
from numpy import asarray
import numpy as np
import pickle

app = Flask(__name__)
 
UPLOAD_FOLDER = 'static/uploads/'
app.secret_key = "secret key"
app.config['UPLOAD_FOLDER'] = UPLOAD_FOLDER
 
ALLOWED_EXTENSIONS = set(['png', 'jpg', 'jpeg', 'gif'])
 
classes = ['airplane','automobile','bird','cat','deer','dog','frog','horse','ship','truck']

my_model=pickle.load(open('model.pkl','rb'))

def convert(image):
    img=Image.open(image)
    img=img.resize((32,32),Image.ANTIALIAS)
    return img

def classify(filename):
    image=convert(app.config['UPLOAD_FOLDER']+filename)
    matrix=asarray(image)
    fd , hog_im = hog(matrix , orientations=9 , pixels_per_cell = (8,8), cells_per_block = (2,2) , visualize = True ,  multichannel = True)
    print(matrix.shape)
    prdct = my_model.predict(fd.reshape(1, -1))[0]
    return classes[prdct]

def allowed_file(filename):
    return '.' in filename and filename.rsplit('.', 1)[1].lower() in ALLOWED_EXTENSIONS
     
 
@app.route('/')
def home():
    return render_template('index.html')
 
@app.route('/', methods=['POST'])
def upload_image():
    if request.method == 'POST':
        file = request.files['file']
        
        if file and allowed_file(file.filename):
            filename = secure_filename(file.filename)
            file.save(os.path.join(app.config['UPLOAD_FOLDER'], filename))
            print('upload_image filename: ' + filename)
            # flash('Image successfully uploaded and displayed below')
            pred=classify(filename)
            flash(f'Prediction is {pred}')
            return render_template('index.html', filename=filename)
        else:
            flash('Allowed image types are - png, jpg, jpeg, gif')
            return redirect(request.url)

@app.route('/display/<filename>')
def display_image(filename):
    print('display_image filename: ' + filename)
    print('./files/' + filename)
    return redirect(url_for('static', filename='uploads/' + filename))

if __name__ == "__main__":
    app.run(debug=True)
    
    
    