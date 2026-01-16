import os
from flask import Flask,render_template,request,send_from_directory
from werkzeug import secure_filename
app=Flask(__name__,static_folder='photos')
UPLOAD_FOLDER='/home/ganesh/Desktop/Projects/exp/photos'
p=UPLOAD_FOLDER
ALLOWED_EXTENSIONS=set(['png','jpg','jpeg'])
app.config['UPLOAD_FOLDER']=UPLOAD_FOLDER
@app.route('/')

def input():
    return render_template('d.html')

@app.route('/save',methods=['GET','POST'])


def save():
    if request.method=='POST':
        f=request.files['image']
        name=(f.filename).split('.')
        if name[1] in ALLOWED_EXTENSIONS:
            filename=secure_filename(f.filename)
            f.save(os.path.join(app.config['UPLOAD_FOLDER'],filename))
            k=f.filename
            return render_template('h.html')
        else:
            return 'File extension not allowed '

import shutil
@app.route('/met',methods=['POST','GET'])
def met():
    import paralleldots
    import json
    api_key="zIAZOZfZvvLW6luxNluGHa0Pvt623evzdR42paLpWNY"
    paralleldots.set_api_key(api_key)
    p1="/home/ganesh/Desktop/Projects/exp/photos/"
    p2=os.listdir(p1)
    p1=p1+str(p2[0])
    k=paralleldots.facial_emotion(p1)
    if "No face detected." in k:
        shutil.rmtree('/home/ganesh/Desktop/Projects/exp/photos')
        os.mkdir('/home/ganesh/Desktop/Projects/exp/photos')
        return render_template('error.html')
    else:
        m=0
        mv="p"
        if 'facial_emotion' not in k:
            shutil.rmtree('/home/ganesh/Desktop/Projects/exp/photos')
            os.mkdir('/home/ganesh/Desktop/Projects/exp/photos')
            return render_template('error.html')
        for j in k['facial_emotion']:
            e=j['tag']
            if j['score']>m:
                mv=e
                m=j['score']
    shutil.rmtree('/home/ganesh/Desktop/Projects/exp/photos')
    os.mkdir('/home/ganesh/Desktop/Projects/exp/photos')
    return render_template('success.html',emotion=mv)