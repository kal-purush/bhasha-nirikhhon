from flask import Flask, render_template

from get_tweets import get_news

application = Flask(__name__)
application.config['SECRET_KEY'] = 'H1dd3nKey'


@application.route('/')
def main_page():
    """
    This get endpoint is the homepage for the web app
    Accessible using 127.0.0.1:5000
    """
    contents = get_news()
    return render_template('main.html', contents=contents)


if __name__ == '__main__':
    application.run()