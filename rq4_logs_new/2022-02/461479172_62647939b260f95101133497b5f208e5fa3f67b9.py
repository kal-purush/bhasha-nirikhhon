import pyrebase


config = {
  "apiKey": "AIzaSyCmAQZocPKYvVLpq4pYxBpEx6ITthsfPQI",
  "authDomain": "messattendance-fc208.firebaseapp.com",
  "projectId": "messattendance-fc208",
  "storageBucket": "messattendance-fc208.appspot.com",
  "messagingSenderId": "932494920430",
  "appId": "1:932494920430:web:0125f906b4f578aed181ff",
  "databaseURL" : "https://messattendance-fc208-default-rtdb.asia-southeast1.firebasedatabase.app/"
}

firebase = pyrebase.initialize_app(config)

db = firebase.database()

data = {
    "name": "Mess Attendace System"
}

db.child('Breakfast').push(data)