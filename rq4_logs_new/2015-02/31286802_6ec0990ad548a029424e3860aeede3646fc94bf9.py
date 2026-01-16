
from app import app
import psycopg2
import os
from flask import session,flash,render_template, request, redirect, url_for


con = None

@app.route('/')
def home():
    """Render website's home page."""
    return render_template('home.html')






@app.route('/profile',methods=['POST','GET'])
def profile():
    f = request.files['file']
    filename = f.filename
    fname=request.form['fname']
    lname=request.form['lname']
    filepath="/info3180/info3180project1/uploads"+filename
    if request.method=="POST":
        if request.form['fname']=="":
            error="please enter first name"
        elif request.form['lname']=="":
            error="enter last name"
        else:
             f.save(os.path.join("uploads", filename))
             con = psycopg2.connect(dbname='infodatabase' user='hevongordon')
             cur = con.cursor()
  
             cur.execute("CREATE TABLE profile (Id INTEGER PRIMARY KEY,FirstName VARCHAR(20),Lastname VARCHAR(20), imagepath VARCHAR(500)")
             cur.execute("INSERT INTO profile VALUES(NULL,fname,lname,filepath)")
    
             con.commit()
            

    return  render_template('create_profile.html',error='error')






@app.route('/profiles',methods=["GET"])
def profiles():
   return render_template('view_profile.html')







app.route('profile/<userid>',methods=["POST"])
def profile():
    return render_template('view_profile_list')








if __name__ == '__main__':
    app.run(debug=True,host="0.0.0.0",port="8888")