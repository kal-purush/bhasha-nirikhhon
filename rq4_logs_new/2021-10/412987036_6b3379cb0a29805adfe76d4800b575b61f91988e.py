import requests
from flask import Flask, render_template, request,session
import os
import mysql.connector

app = Flask(__name__)
app.secret_key=os.urandom(24)

@app.route('/taxi',methods=['POST','GET'])
def taxi():
    try:
        mydb = mysql.connector.connect(host="127.0.0.1",
                                user="root",
                                password="root",
                                database="local_conveyance",
                                auth_plugin='mysql_native_password')
        mycursor = mydb.cursor()
        if True:
            query="select * from taxi"
            mycursor.execute(query)
            taxis=mycursor.fetchall()
            return render_template('taxi.html',taxis=taxis)
        else:
            return render_template('home.html')
    except Exception as e:
        return(str(e))

@app.route('/book/taxi/<string:tx_id>',methods=['GET','POST'])
def book_taxi(tx_id):
    try:
        mydb = mysql.connector.connect(host="127.0.0.1",
                                user="root",
                                password="root",
                                database="local_conveyance",
                                auth_plugin='mysql_native_password')
        mycursor = mydb.cursor()
        if True :
            query="select taxi_number,operational_city,rate_km from taxi where tx_id=%s"
            rec_tup=(tx_id,)
            mycursor.execute(query,rec_tup)
            t=mycursor.fetchone()
            taxi_number=t[0]
            oper_city=t[1]
            rate_km=t[2]
            username = "user1"
            query2="insert into taxi_bookings(username,tx_id) values (%s,%s)"
            rec_tup=(username,tx_id)
            mycursor.execute(query2,rec_tup)
            mydb.commit()
            query3="select booking_id from taxi_bookings where username=%s"
            rec_tup1=(username,)
            mycursor.execute(query3,rec_tup1)
            records=mycursor.fetchall()
            length=len(records)
            last_record=records[length-1]
            return render_template('taxi_confirm.html',id=last_record[0],other=rec_tup)
        else:
            return render_template('home.html')

    except Exception as e:
        return(str(e))

if __name__ == '__main__':
    app.run(host="localhost", port=8003, debug=True)