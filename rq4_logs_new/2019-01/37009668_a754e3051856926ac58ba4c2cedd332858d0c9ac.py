import os
import re
import sys
from string import letters
import webapp2
import jinja2

from google.appengine.ext import db

template_dir = os.path.join(os.path.dirname(__file__), 'jinja2_templates')
jinja_env = jinja2.Environment(loader = jinja2.FileSystemLoader(template_dir),
	autoescape = True)

art_key = db.Key.from_path('ASCIIChan', 'arts')

def console(s):
	"""Writes an error message (s) to the console."""
	sys.stderr.write('%s\n' % s)

console(art_key)

class Handler(webapp2.RequestHandler):
  def write(self, *a, **kw):
  	self.response.write(*a, **kw)

  def render_str(self, template, **params):
  	t = jinja_env.get_template(template)
  	return t.render(params)

  def render(self, template, **kw):
  	self.write(self.render_str(template, **kw))

class Art(db.Model):
	title = db.StringProperty(required = True)
	art = db.TextProperty(required = True)
	created = db.DateTimeProperty(auto_now_add = True)

class MainPage(Handler):
	def render_front(self, error = '', title = '', art = ''):
		arts = db.GqlQuery(
			"""SELECT * FROM Art WHERE ANCESTOR IS :1 ORDER BY created LIMIT 10""", art_key)

		self.render('front.html', title=title, art=art, error=error, arts=arts)

	def get(self):
		return self.render_front()

	def post(self):
		title = self.request.get('title')
		art = self.request.get('art')
		if title and art:
			p = Art(parent = art_key, title=title, art=art)
			p.put()
			self.redirect('/ascii-chan-ipnd')
		else:
			error = "we need both a title and some artwork!"
			self.render_front(error=error, title=title, art=art)

app = webapp2.WSGIApplication([('/ascii-chan-ipnd', MainPage)], debug = True)