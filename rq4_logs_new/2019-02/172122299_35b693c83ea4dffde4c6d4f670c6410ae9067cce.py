from flask import Flask
import requests
import json
from flask import Response,render_template,request
import psycopg2 as psql
from datetime import datetime
import os
import base64
from io import StringIO
import PIL.Image
import urllib.parse
app=Flask(__name__)

@app.route("/listCategories/<name>")
def category(name):
    name=urllib.parse.unquote(name)
    r=requests.get("http://3.93.233.142:5000/api/v1/categories/"+name+"/acts")
    if(r.status_code==204):
        return render_template("each_category.html",error="No content to display")
    elif(r.status_code!=200):
        return render_template("each_category.html",error="Error! "+str(r.status_code))
    act_data=json.loads(r.text)
    print("received act_data",act_data)
    return render_template("each_category.html",category=name,data=act_data)

@app.route("/upvote/<category>/<id>")
def upvote_wrapper(category,id):
    category1=urllib.parse.unquote(category)
    l=[]
    l.append(str(id))
    r=requests.post("http://3.93.233.142:5000/api/v1/acts/upvote",json=l)
    if(r.status_code==200):
        print("Category name",category1)
        r=requests.get("http://3.93.233.142:5000/api/v1/categories/"+category1+"/acts")
        if(r.status_code==204):
            return render_template("each_category.html",error="No content to display")
        act_data=json.loads(r.text)
        print("received act_data",act_data)
        return render_template("each_category.html",category=category1,data=act_data)
    if(r.status_code!=200):
        return render_template("each_category.html",error="Failed!")
        #return Response("{Failed:failed",status=r.status_code,mimetype="application/json")
@app.route("/")
def listCategories():
    r=requests.get("http://3.93.233.142:5000/api/v1/categories")
    if(r.status_code!=200):
        #No content
        return render_template("categories.html")
    return render_template("categories.html",data=json.loads(r.text))
    #return r.json()
@app.route("/actSubmit",methods=['POST'])
def actSubmit():
     if request.method!='POST':
         return Response("{}",status=405,mimetype="application/json")
     description=request.form['description']
     categoryName=request.form['category']
     upvotes=0
     r=requests.get("http://3.93.233.142:5000/api/numActs")
     if(r.status_code==204):
         return render_template("categories.html",error="Bad request")
     act_id=int(json.loads(r.text)["numActs"])+1
     timestamp=datetime.now().strftime("%d-%m-%Y:%S-%M-%H")
     username=request.form["username"]

     data_from_form=dict()
     data_from_form["caption"]=description
     data_from_form["categoryName"]=categoryName
     data_from_form["actId"]=act_id
     data_from_form["timestamp"]=timestamp
     data_from_form["username"]=username
     img=request.files["file"]

     data_from_form["imgB64"]=base64.b64encode(img.read())
     data_from_form["imgB64"]=data_from_form["imgB64"].decode('utf-8')
     r=requests.post("http://3.93.233.142:5000/api/v1/acts",json=data_from_form)
     if(r.status_code!=201):
         #Bad request
         return render_template("categories.html",error="Bad request")
     #query="SELECT COUNT(*) FROM acts;"
     #Get unique act id
     #cursor.execute(query)
     #res=cursor.fetchone()
     #act_id=0
     #if res is None:
     #    act_id=0
     #act_id=int(res[0])+1
     #Name of the tag containing the upload button is file

     #img=request.files['file']
     #imgurl=img_path+str(act_id)+".png"

     #img.save(imgurl)
     # img.save(secure_filename(img.filename))
     #timestamp=datetime.datetime.fromtimestamp(datetime.datetime.now()).strftime(                                                                                      '%Y-%m-%d %H:%M:%S')
     #timestamp=datetime.datetime.now()
     else:
         return render_template("categories.html",success="Success!")

if __name__=="__main__":
    app.run(debug=True,host='0.0.0.0')