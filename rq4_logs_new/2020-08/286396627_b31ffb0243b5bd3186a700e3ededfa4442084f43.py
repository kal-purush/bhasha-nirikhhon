from flask import Flask, render_template, request, flash, session
from flask_sqlalchemy import SQLAlchemy
import re
from flask_mail import Mail, Message
import utils
import pymongo


app = Flask(__name__)
app.secret_key = "hello"
app.config["SQLALCHEMY_DATABASE_URI"] = 'sqlite:///users.sqlite3'
app.config["SQLALCHEMY_TRACK_MODIFICATION"] = False

app.config.update(
	DEBUG=True,
	#EMAIL SETTINGS
	MAIL_SERVER='',
	MAIL_PORT=465,
	MAIL_USE_SSL=True,
	MAIL_USE_TLS = False,
	MAIL_USERNAME = '',
	MAIL_PASSWORD = ''
	)
mail = Mail(app)

db = SQLAlchemy(app)

class users(db.Model):
	_id = db.Column("id",db.Integer, primary_key = True)
	email = db.Column(db.String(100))
	status = db.Column(db.Integer)
	nb_day =  db.Column(db.Integer)

	def __init__(self, email, status, nb_day):
		self.email = email
		self.status = status
		self.nb_day = nb_day

@app.route("/") 
def home():
	return render_template("index.html")

@app.route("/suscribe", methods=["POST", "GET"]) 
def suscribe():
	try:
		if request.method == "POST":
			user = request.form["email"]
			status = request.form["status"]
			nb_day = request.form["nb_day"]
			if utils.check_email(user) == True and utils.check_status(status,["0"]) == True and utils.check_nb_days(nb_day) == True:
				found_user = users.query.filter_by(email=user).first()
				msg_object = "welcome"
				sender_address = "reuglewiczjeanedouard@gmail.com"
				msg_body = "welcome to the newsletter"
				utils.insert_user(found_user,users,user,status,nb_day,db)
				print(user)
				utils.send_email(user,sender_address,msg_body,msg_object)
				return render_template("suscribe.html")
			else:
				flash("Some informations are missing or incorrect","info")
				return render_template("suscribe.html")
		else:
			return render_template("suscribe.html")
	except:
		return render_template("suscribe.html")


@app.route("/update")
def update():
	return render_template("update.html")

@app.route("/update_account", methods=["POST", "GET"])
def update_profile():
	try:
		if request.method == "POST":
			user = request.form["email"]
			status = request.form["status"]
			sender_address = ''
			utils.update_account(db,users,user,status,sender_address)
			return render_template("update_account.html")
		else:
			return render_template("update_account.html")
	except:
		return render_template("update_account.html")

@app.route('/show_all')
def show_all():
   return render_template('show_all.html', users = users.query.all() )


if __name__ == "__main__":
	db.create_all()
	app.debug = True
	app.run()