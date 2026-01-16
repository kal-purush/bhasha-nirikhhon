# export MAIL_USERNAME=jessicasi364@gmail.com ADMIN=jessicasi364@gmail.com MAIL_PASSWORD=bos11lits12

import requests
import os
from flask import Flask, render_template, session, redirect, url_for, flash, jsonify, send_from_directory
from flask_script import Manager, Shell
from flask_wtf import FlaskForm
from wtforms import StringField, SubmitField, FileField, PasswordField, BooleanField, SelectMultipleField, ValidationError
from wtforms.validators import Required, Length, Email, Regexp, EqualTo
from flask_sqlalchemy import SQLAlchemy

from flask_migrate import Migrate, MigrateCommand

from flask_mail import Mail, Message
from threading import Thread
from werkzeug import secure_filename

from werkzeug.security import generate_password_hash, check_password_hash
from flask_login import LoginManager, login_required, logout_user, login_user, UserMixin, current_user

from flask import Flask, request
app = Flask(__name__)
app.debug = True 

# Configure base directory of app
basedir = os.path.abspath(os.path.dirname(__file__))

# Application configurations
app = Flask(__name__)
app.debug = True
app.config['SECRET_KEY'] = 'hardtoguessstringfromsi364(thisisnotsupersecure)'
## Create a database in postgresql in the code line below, and fill in your app's database URI. It should be of the format: postgresql://localhost/YOUR_DATABASE_NAME

## TODO: Create database and change the SQLAlchemy Database URI.
## Your Postgres database should be your uniqname, plus HW5, e.g. "jczettaHW5" or "maupandeHW5"
app.config["SQLALCHEMY_DATABASE_URI"] = "postgresql://localhost/final"
app.config['SQLALCHEMY_COMMIT_ON_TEARDOWN'] = True
app.config['SQLALCHEMY_TRACK_MODIFICATIONS'] = False

# TODO: Add configuration specifications so that email can be sent from this application, like the examples you saw in the textbook and in class. Make sure you've installed the correct library with pip! See textbook.
# NOTE: Make sure that you DO NOT write your actual email password in text!!!!
# NOTE: You will need to use a gmail account to follow the examples in the textbook, and you can create one of those for free, if you want. In THIS application, you should use the username and password from the environment variables, as directed in the textbook. So when WE run your app, we will be using OUR email, not yours.

app.config['MAIL_SERVER'] = 'smtp.googlemail.com'
app.config['MAIL_PORT'] = 587 #default
app.config['MAIL_USE_TLS'] = True
app.config['MAIL_USERNAME'] = os.environ.get('MAIL_USERNAME') 
app.config['MAIL_PASSWORD'] = os.environ.get('MAIL_PASSWORD')
app.config['MAIL_SUBJECT_PREFIX'] = '[FAA Delays]'
app.config['MAIL_SENDER'] = 'Admin jessicasi364' # TODO fill in email
app.config['ADMIN'] = os.environ.get('ADMIN')

UPLOAD_FOLDER = 'prof_pics/'
ALLOWED_EXTENSIONS = set(['png'])

app.config['UPLOAD_FOLDER'] = UPLOAD_FOLDER

# Set up Flask debug stuff
manager = Manager(app)
db = SQLAlchemy(app) # For database use
migrate = Migrate(app, db) # For database use/updating
manager.add_command('db', MigrateCommand) # Add migrate
# TODO: Run commands to create your migrations folder and get ready to create a first migration, as shown in the textbook and in class.
mail = Mail(app)

login_manager = LoginManager()
login_manager.session_protection = 'strong'
login_manager.login_view = 'login'
login_manager.init_app(app) # set up login manager


## Set up Shell context so it's easy to use the shell to debug
def make_shell_context():
	return dict(app=app, db=db, Tweet=Tweet, User=User, Hashtag=Hashtag)
# Add function use to manager
manager.add_command("shell", Shell(make_context=make_shell_context))

# TODO: Write a send_email function here. (As shown in examples.)

def send_async_email(app, msg):
	with app.app_context():
		mail.send(msg)

def send_email(to, subject, template, **kwargs):  
	msg = Message(app.config['MAIL_SUBJECT_PREFIX'] + ' ' + subject,
				  sender=app.config['MAIL_SENDER'], recipients=[to])
	msg.body = render_template(template + '.txt', **kwargs)
	msg.html = render_template(template + '.html', **kwargs)
	thr = Thread(target=send_async_email, args=[app, msg]) 
	thr.start()
	return thr 

#user_search_history = db.Table('user_searches', db.Column('user_id', db.Integer, db.ForeignKey('users.id')), db.Column('airport_id', db.Integer, db.ForeignKey('airportcode.id')))

class User(UserMixin, db.Model):
	__tablename__ = "users"
	
	id = db.Column(db.Integer, primary_key=True) 
	username = db.Column(db.String(64), unique=True)
	email = db.Column(db.String(64), unique=True)
	password_hash = db.Column(db.String(200))

	@property
	def password(self):
		raise AttributeError('password is not a readable attribute')

	@password.setter
	def password(self, password):
		self.password_hash = generate_password_hash(password)

	def verify_password(self, password):
		return check_password_hash(self.password_hash, password)

class Airport(db.Model):
	__tablename__ = 'airportcode'
	id = db.Column(db.Integer, primary_key=True)
	abbrev = db.Column(db.String, unique=True) 
	state = db.Column(db.String) 
	name = db.Column(db.String, unique=True) 

class Status(db.Model):
	__tablename__ = 'Weather_Status'
	id = db.Column(db.Integer, primary_key=True)
	airport_id = db.Column(db.Integer, db.ForeignKey('airportcode.id'))
	temp = db.Column(db.String)
	wind = db.Column(db.String)
	status_delay = db.Column(db.String)
	updated = db.Column(db.String)

class User_Search(db.Model):
	__tablename__ = "past_searches"
	id = db.Column(db.Integer, primary_key=True)
	user_id = db.Column(db.Integer, db.ForeignKey('users.id'))
	airport_id = db.Column(db.Integer, db.ForeignKey('airportcode.id'))

@login_manager.user_loader
def load_user(user_id):
	return User.query.get(int(user_id)) # returns User object or None

class RegistrationForm(FlaskForm):
	email = StringField('Email:', validators=[Required(),Length(1,64),Email()])
	username = StringField('Username:',validators=[Required(),Length(1,64),Regexp('^[A-Za-z][A-Za-z0-9_.]*$',0,'Usernames must have only letters, numbers, dots or underscores')])
	password = PasswordField('Password:',validators=[Required(),EqualTo('password2',message="Passwords must match")])
	password2 = PasswordField("Confirm Password:",validators=[Required()])
	submit = SubmitField('Register User')

	#Additional checking methods for the form
	def validate_email(self,field):
		if User.query.filter_by(email=field.data).first():
			raise ValidationError('Email already registered.')

	def validate_username(self,field):
		if User.query.filter_by(username=field.data).first():
			raise ValidationError('Username already taken')

class LoginForm(FlaskForm):
	email = StringField('Email', validators=[Required(), Length(1,64), Email()])
	password = PasswordField('Password', validators=[Required()])
	remember_me = BooleanField('Keep me logged in')
	submit = SubmitField('Log In')


def get_or_create_airport(db_session, abbrev, state, name):
	airport = db_session.query(Airport).filter_by(abbrev = abbrev).first()
	if airport:
		return airport
	else:
		airport = Airport(abbrev = abbrev, state= state, name = name)
		db_session.add(airport)
		db_session.commit()
		return airport


def get_or_create_status(db_session, airport_id, temp, wind, reason, updated):
	sit_rep = db_session.query(Status).filter_by(airport_id=airport_id, updated=updated).first()
	if sit_rep:
		return sit_rep
	else:
		sit_rep = Status(airport_id=airport_id, temp=temp, wind=wind, status_delay=reason, updated=updated)
		db_session.add(sit_rep)
		db_session.commit()
		return sit_rep


def get_weather(airportcode, user):
	data = requests.get("http://services.faa.gov/airport/status/{}?format=json".format(airportcode)).json()
	airport=get_or_create_airport(db.session, airportcode, data["state"], data['name'])
	get_or_create_status(db.session, airport.id, data["weather"]["temp"], data["weather"]["wind"], data["status"]["reason"], data["weather"]["meta"]["updated"])
	new_search = User_Search(airport_id=airport.id, user_id=user.id)
	db.session.add(new_search)
	db.session.commit()
	return data

class AirportSearch(FlaskForm):
	code = StringField('Airport Code:', validators=[Required()])
	submit = SubmitField('Search for Airport')


@app.route('/',methods= ['POST','GET'])
@login_required
def enter_data():
	form = AirportSearch()
	if form.validate_on_submit():
		return redirect("/airport/{}".format(form.code.data))	
	return render_template("template2.html", form=form, username=current_user.username)

## Login routes
@app.route('/login',methods=["GET","POST"])
def login():
	form = LoginForm()
	if form.validate_on_submit():
		user = User.query.filter_by(email=form.email.data).first()
		if user is not None and user.verify_password(form.password.data):
			login_user(user, form.remember_me.data)
			return redirect(request.args.get('next') or url_for('enter_data'))
		flash('Invalid username or password.')
	return render_template('login.html',form=form)

@app.route('/logout')
@login_required
def logout():
	logout_user()
	flash('You have been logged out')
	return redirect(url_for('login'))

@app.route('/register',methods=["GET","POST"])
def register():
	form = RegistrationForm()
	if form.validate_on_submit():
		user = User(email=form.email.data,username=form.username.data,password=form.password.data)
		db.session.add(user)
		db.session.commit()
		flash('You can now log in!')
		return redirect(url_for('login'))
	return render_template('register.html',form=form)


@app.route('/airport/<airport_code>')
@login_required
def airport_data(airport_code):
	return render_template("airport_status.html", data=get_weather(airport_code, current_user))

@app.route('/history')
@login_required	   
def user_history():
	past_searches = db.session.query(User_Search, Airport).filter(User_Search.user_id == current_user.id).join(Airport).all()[::-1]
	return render_template("history.html", searches=past_searches)

@app.route('/email', methods = ["GET", "POST"])
@login_required	   
def email():
	past_searches = db.session.query(User_Search, Airport).filter(User_Search.user_id == current_user.id).join(Airport).all()[::-1]
	if request.method == "POST":
		send_email(current_user.email, "Airport Search History", "mail/history", searches=past_searches)
		return redirect("/")

	return render_template("email.html",  searches=past_searches)

@app.route('/history_api', methods = ["GET"])
@login_required	   
def api():
	data = db.session.query(User_Search, Airport).filter(User_Search.user_id == current_user.id).join(Airport).all()[::-1]
	return jsonify({
		"searchList" : [{"abbrev" : res.Airport.abbrev, "name" : res.Airport.name} for res in data]
		})

def allowed_file(filename):
    return '.' in filename and \
           filename.rsplit('.', 1)[1].lower() in ALLOWED_EXTENSIONS

@app.route('/my_pic', methods=['GET', 'POST'])
def upload_file():
	if request.method == 'POST':
		# check if the post request has the file part
		if 'file' not in request.files:
			flash('No file part')
			return redirect(request.url)
		file = request.files['file']
		# if user does not select file, browser also
		# submit a empty part without filename
		if file.filename == '':
			flash('No selected file')
			return redirect(request.url)
		if file and allowed_file(file.filename):
			filename = secure_filename(file.filename)
			file.save(os.path.join(app.config['UPLOAD_FOLDER'], "{}.png".format(current_user.id)))
	return send_from_directory(app.config['UPLOAD_FOLDER'],"{}.png".format(current_user.id))

@app.errorhandler(404)
def page_not_found(e):
	return render_template('404.html'), 404

@app.errorhandler(500)
def internal_server_error(e):
	return render_template('500.html'), 500

if __name__ == '__main__':
	db.create_all()
	manager.run() # Run with this: python main_app.py runserver
	# Also provides more tools for debugging