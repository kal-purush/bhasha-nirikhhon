# =============================================================
# Created Date : 19/1/2017
# Name : app.py
# Contributers : Ajay
# =============================================================

from bottle import route, run, template
from bottle import Bottle
from bottle import request, response, route
from sqlalchemy import create_engine,asc
from sqlalchemy.orm import sessionmaker

from dbsetup import Base,session
# import class form cache.py
from cache import MyCache
import uuid

Cache = MyCache()
engine = create_engine('sqlite:///session.db')
Base.metadata.bind = engine
DBSession = sessionmaker(bind=engine)
sessions = DBSession()
 
''' @verifyToken decoratotor for validating accesstokens '''
def verifyToken(func):
   def verify():    
      f = func()  
      authToken = request.forms.token 
      '''Check for cached token, if not make a db call  '''
      if Cache.__contains__(authToken):
         return f
      else:
         try:
            sessions.query(session).filter_by(token = authToken).one()
            Cache.update(authToken)
            return f
         except:
            return 'Access Denied'
   return verify


''' Redirects to the home template '''
@route('/')
def home():
    return template('form.html')


''' Get all tokens from the database '''

@route('/getAllTokens',method="POST")
@verifyToken
def getAllTokens():
    print request.forms.token;    
    retStr = '' 
    for i in sessions.query(session.token):
        retStr += '<li>'+i[0]+'</li>'  
    return retStr


'''Generate new access token '''

@route('/getNewToken',method="GET")
def getNewToken():
    uid = str( uuid.uuid4() )
    t1 = session(token = uid )
    sessions.add(t1)
    sessions.commit()
    Cache.update(uid)
    return 'Your New Token : '+uid


run(host='localhost', port=8080)