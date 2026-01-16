from flask import Flask,render_template,request
import mysql.connector

app=Flask(__name__)


@app.route('/')
def about():
    render_template('index.html')


@app.route('/results',methods=['GET','POST'])
def result():
    mydb=mysql.connector.connect(
        host="localhost",
        user="root",
        password="",
        database="marvellous"
    )
    mycursor=mydb.cursor()
    if request.method=='POST':
        signup=request.form
        username=signup['user']
        email=signup['email']
        password=signup['pass']
        mycursor.execute("insert into reg (user,email,pass)values(%s,%s,%s)",(username,email,password))
        mydb.commit()
        mycursor.close()
        return "Register Successfull"


if __name__=="__main__":
     app.run(debug=True)