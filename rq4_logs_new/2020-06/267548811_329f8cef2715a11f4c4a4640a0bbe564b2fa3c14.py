from application import app
from flask import request, Response
from random import randint

@app.route('/city/country', methods = ['POST'])
def country():
    city =  request.data.decode('utf-8')
    if city == 'London':
        country = 'England'
    elif city == 'Paris':
        country = 'France'
    elif city == 'Tokyo':
        country = 'Japan'
    elif city == 'New York':
        country = 'USA'
    elif city == 'Vaduz':
        country = 'Liechtenstein'
    elif city == 'Luxembourg City':
        country = 'The Grand Duchy of Luxembourg'
    elif city == 'Sydney':
        country = 'Australia'
    elif city == 'Moscow':
        country 'Russia'
    elif city == 'Geneva':
        country = 'Switzerland'
    elif city == 'Madrid':
        country = 'Spain'
    elif city == 'Edinburgh':
        country = 'Scotland'
    elif city == 'Beijing':
        country = 'China'
    elif city == 'Mumbai':
        country = 'India'
    elif city == 'Berlin':
        country = 'Germany'
    elif city == 'Cape Town':
        country = 'South Africa'
    elif city == 'Rio de Janeiro':
        country = 'Brazil'
    elif == 'Toronto':
        country = 'Canada'    
    else:
        city = 'City not recognised'
    return Response(country, mimetype='text/plain')