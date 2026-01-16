#import Declarations
import datetime
from os import environ
from flask import Flask, render_template

app = Flask(__name__)

#HTML page routing

#Homepage Route-
@app.route('/')
@app.route('/home')
def home():
    """Renders the home page."""
    return render_template(
        'home.html',
        title='Home Page',
        year=datetime.datetime.now().year,
    )

#Contact Route-
@app.route('/contact')
def contact():
    """Renders the contact page."""
    return render_template(
        'contact.html',
        title='Contact',
        year=datetime.datetime.now().year,
        message='Send us a mail at'
    )

#About Route-   
@app.route('/about')
def about():
    """Renders the about page."""
    return render_template(
        'about.html',
        title='Products',
        year=datetime.datetime.now().year,
        message='Your application description page.'
    )

#Products Route-
@app.route('/products')
def products():
    """Renders the product page."""
    return render_template(
        'products.html',
        title='About',
        year=datetime.datetime.now().year,
        message='Your application description page.'
    )

#Example-
#@app.route('/APP_PY_ROUTER')
#def APP_PY_ROUTER():
#    """Renders the product page."""
#    return render_template(
#        'PAGE.html',
#        title='PAGE TITLE',
#        year=datetime.datetime.now().year,
#        message='Your application description page.'
#    )


### Application runner ###
if __name__ == '__main__':
    HOST = environ.get('SERVER_HOST', 'localhost')
    try:
        PORT = int(environ.get('SERVER_PORT', '5555'))
    except ValueError:
        PORT = 5555
    app.run(HOST, PORT,debug=True)