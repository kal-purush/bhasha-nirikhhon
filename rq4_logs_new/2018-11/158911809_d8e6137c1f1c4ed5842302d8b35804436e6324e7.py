import os

from flask import Flask, render_template
# move to ./controllers when production ready
from . import db, auth, profile

def create_app(test_config=None):
	# Creates & configs
	app = Flask(__name__, instance_relative_config=True)
	app.config.from_mapping(
		SECRET_KEY='dev', # randomize this in production
		DATABASE=os.path.join(app.instance_path, 'birdcraft.sqlite'),
	)

	if test_config is None:
		# load the instance config, if it exists, when not testing
		app.config.from_pyfile('config.py', silent=True)
	else:
		# load the test config if passed in
		app.config.from_mapping(test_config)

	# ensure the instance folder exists
	try:
		os.makedirs(app.instance_path)
	except OSError:
		pass

	"""
	see http://flask.pocoo.org/docs/1.0/tutorial/templates/
	tldr: {{}} expressions outputted to document,
		{% %} control statements like for and if
	"""
	# Calls db.py and initializes database
	# Run `flask init-db` to initalize the database
	db.init_app(app)

	@app.errorhandler(404)
	def not_found(error):
		return render_template('404.html'), 404

	# Calls auth.py and initializes the blueprint
	# Read more: http://flask.pocoo.org/docs/1.0/blueprints/
	app.register_blueprint(auth.bp)

	# Profile.bp handles root route
	app.register_blueprint(profile.bp)
	app.add_url_rule('/', endpoint='index')

	return app