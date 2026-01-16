import webapp2
import jinja2
import os
import logging
import time
from google.appengine.api import users
from google.appengine.ext import ndb

env = jinja2.Environment(
    loader=jinja2.FileSystemLoader(os.path.dirname(__file__)),
    extensions=["jinja2.ext.autoescape"],
    autoescape=True)

# Datastore/Database
class Person(ndb.Model):
    name = ndb.StringProperty()
    bio = ndb.StringProperty()
    birthday = ndb.DateProperty()
    email = ndb.StringProperty()

class MainPage(webapp2.RequestHandler):
    def get(self):
        # 1. Read the request
        current_user = users.get_current_user()
        # 2. Read and write from the database
        people = Person.query().fetch()
        if current_user:
            current_email = current_user.email()
            current_person = Person.query().filter(Person.email == current_email).get()
        else:
            current_person = None
        # 3. Render a response
        logout_url = users.create_logout_url("/")
        login_url = users.create_login_url("/")
        templateVars = {
            "people": people,
            "current_user": current_user,
            "logout_url": logout_url,
            "login_url": login_url,
            "current_person": current_person,
        }
        template = env.get_template("templates/home.html")
        self.response.write(template.render(templateVars))

class ProfilePage(webapp2.RequestHandler):
    def get(self):
        # 1. Get info from the request
        urlsafe_key = self.request.get("key")
        # 2. Read and write from the database
        key = ndb.Key(urlsafe=urlsafe_key)
        person = key.get()
        templateVars = {
            "person": person,
        }
        # 3. Render a response
        template = env.get_template("templates/profile.html")
        self.response.write(template.render(templateVars))

class CreateHandler(webapp2.RequestHandler):
    def post(self):
        # 1. Get info from the request
        name = self.request.get("name")
        bio = self.request.get("bio")
        current_user = users.get_current_user()
        email = current_user.email()
        # 2. read or write the database
        person = Person(name=name, bio=bio, email=email)
        person.put()
        # 3. Render a reponse
        time.sleep(2)
        self.redirect("/")

app = webapp2.WSGIApplication([
    ("/", MainPage),
    ("/profile", ProfilePage),
    ("/create", CreateHandler)
], debug=True)