from flask import Flask,render_template,request,jsonify
from flask_pymongo import PyMongo

app = Flask(__name__)
app.config["MONGO_URI"] = "mongodb+srv://vrushti9421:UFNmPIfd92HNW9PR@cluster0pt.jzcfs46.mongodb.net/chatgpt"
mongo = PyMongo(app)
mongo = PyMongo(app)

@app.route('/',methods=['GET', 'POST'])
def home():
    if request.method == 'POST':
        name = request.form.get('name')
        email = request.form.get('email')
        message = request.form.get('message')

        # Insert the note into MongoDB
        contact_data = {'name': name, 'email': email, 'message': message}
        mongo.db.contactMe.insert_one(contact_data)

    return render_template("index.html")

@app.route('/about_me')
def about_me():
    return render_template('index.html')

@app.route('/projects')
def projects():
    return render_template('index.html')

@app.route('/contact_me')
def contact_me():
    return render_template('index.html')
    

app.run(debug=True)