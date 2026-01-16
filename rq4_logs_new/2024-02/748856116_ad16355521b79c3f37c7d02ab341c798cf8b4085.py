#!/usr/bin/env python3
"""This module function creates a basic flask app"""

from datetime import datetime
from flask import g, Flask, render_template, request
from flask_babel import Babel, get_locale
from flask_babel import gettext as _, format_datetime
import pytz


app = Flask(__name__)
babel = Babel(app)

users = {
        1: {"name": "Balou", "locale": "fr", "timezone": "Europe/Paris"},
        2: {"name": "Beyonce", "locale": "en", "timezone": "US/Central"},
        3: {"name": "Spock", "locale": "kg", "timezone": "Vulcan"},
        4: {"name": "Teletubby", "locale": None, "timezone": "Europe/London"},
}


class Config:
    """This class provides the configuration of languages and timezone"""
    LANGUAGES = ["en", "fr"]
    BABEL_DEFAULT_LOCALE = "en"
    BABEL_DEFAULT_TIMEZONE = "UTC"


app.config.from_object(Config)


def get_user(user_id):
    """Retrieve user information based on the user ID"""
    return users.get(int(user_id))


@app.before_request
def before_request():
    """Set the global user variable 'g.user' before each request"""
    login_as = request.args.get('login_as')
    if login_as:
        g.user = get_user(login_as)
    else:
        g.user = None


def get_locale():
    """
    Get the preferred locale for the user.

    The order of priority is:
        1. Locale from URL parameters
        2. Locale from user settings
        3. Locale from request header
        4. Default locale

    Returns:
        str: The preferred locale for the user.
    """
    # Check if locale is provided in URL parameters
    param = request.args.get('locale')
    if param and param in app.config['LANGUAGES']:
        return param

    # Check if user is logged in and has a preferred locale
    if g.user and 'locale' in g.user and g.user[
            'locale'] in app.config['LANGUAGES']:
        return g.user['locale']

    # Check request header for preffered locale
    header_locale = request.headers.get('Accept-Language')
    if header_locale:
        for lang in header_locale.split(','):
            lang = lang.split(';')[0].strip()
            if lang in app.config['LANGUAGES']:
                return lang

    return app.config['BABEL_DEFAULT_LOCALE']


def get_tmz():
    """
    Get the appropriate time zone for the user.

    The order of priority is:
        1. Timezone from URL parameters
        2. Timezone from user settings
        3. Default to UTC

    Returns:
        str: The appropriate time zone for the user.
    """
    # Check if timezone is provided in URL parameters
    tmz = request.args.get('timezone')
    if tmz:
        try:
            pytz.timezone(tmz)
            return tmz
        except pytz.exceptions.UnknownTimeZoneError:
            pass

    # Check if user is logged in and has a preferred timezone
    if g.user and 'timezone' in g.user:
        try:
            pytz.timezone(g.user['timezone'])
            return g.user['timezone']
        except pytz.exceptions.UnknownTimeZoneError:
            pass

    # Default to UTC if no valid timezone is found
    return 'UTC'


def get_timezone():
    """Get the appropriate time zone for the user."""
    return get_tmz()


@app.route('/', methods=['GET'])
def hello():
    """Render the index.html template with the current time
    """
    current_time = datetime.now(pytz.timezone(get_timezone()))
    formatted_time = format_datetime(current_time, format='medium')
    return render_template('7-index.html', current_time=formatted_time)


if __name__ == '__main__':
    app.run(host='0.0.0.0')