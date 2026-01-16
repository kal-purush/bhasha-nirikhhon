import os
from app import app
from flask import render_template
from app.models import Weather


@app.route('/front')
def front():
	forecast = Weather  # get an instance of the class
	forecast.getFront()
	return render_template('front.html', title='Weather Front', forecast=forecast)

if __name__ == '__main__':
    app.run(debug=app.config['DEBUG'])