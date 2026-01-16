from google.appengine.ext import ndb

class Animals(ndb.Model):
    animalQuestion = ndb.StringProperty(required=True)
    animalCorrect = ndb.StringProperty(required=True)
    animalWrong = ndb.StringProperty(repeated=True)

class Geography(ndb.Model):
    geoQuestion = ndb.StringProperty(required=True)
    geoCorrect = ndb.StringProperty(required=True)
    geoWrong = ndb.StringProperty(repeated=True)