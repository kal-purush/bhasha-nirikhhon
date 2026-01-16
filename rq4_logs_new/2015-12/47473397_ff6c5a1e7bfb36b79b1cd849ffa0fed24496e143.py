from flask import Flask, render_template, request
from forms import ContactForm
from flask.ext.mail import Message, Mail

app = Flask(__name__)

app.secret_key = 'WebDesign'

app.config.update(dict(
    DEBUG = True,
    MAIL_SERVER = 'smtp.gmail.com',
    MAIL_PORT = 587,
    MAIL_USE_TLS = True,
    MAIL_USE_SSL = False,
    MAIL_USERNAME = 'umsiwebdesign@gmail.com',
    MAIL_PASSWORD = '105sstate',
))

mail = Mail(app)

@app.route('/')
def home():
  return render_template('home.html')

@app.route('/about')
def about():
  return render_template('about.html')

@app.route('/contact', methods=['GET', 'POST'])
def contact():
  form = ContactForm()

  if request.method == 'POST':
  	msg = Message("Hello From Flask", sender="colleenvanlent@gmail.com", recipients=["colleenvanlent@gmail.com"])
  	mail.send(msg)
  	return 'Form posted.'

  elif request.method == 'GET':
    return render_template('contact.html', form=form)

if __name__ == '__main__':
  app.run(debug=True)