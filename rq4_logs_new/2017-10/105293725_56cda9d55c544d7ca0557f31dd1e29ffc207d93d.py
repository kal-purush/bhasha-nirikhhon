from flask import Flask, render_template
from mixer.backend.flask import mixer

app = Flask(__name__)
app.jinja_env.auto_reload = True
app.config.from_object('config')

@app.route('/')
def landing_page():
   return render_template('landing_page.html', tEvents=events)

from app.models import Event

with app.app_context():
   events = mixer.cycle(4).blend(Event, name=(n for n in ('Back to School', 'Renovating', 'Race to end Poverty')))