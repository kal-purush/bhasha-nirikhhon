#!/usr/bin/env python

################################################################################
#                                                                              #
#  04/13/2015                                                                  #
#  Mobile Sensing Assignment 6                                                 #
#  Tornado server to handle post request that acepts a .png picture            #
#                                                                              #
################################################################################
   
import base64
import json
import os
import socket
import tornado.ioloop
import tornado.web

from bson.binary import Binary
from pymongo import MongoClient
from tornado.escape import recursive_unicode

#address = "127.0.0.1"
address = "104.150.120.136"

port = 8000
base_path = "/home/ubuntu/msd/"
base_image_path = "/home/ubuntu/msd/pictures/"
#base_path = "/Users/elena/"
#base_image_path = "/Users/elena/Desktop/pictures/"

# Code taken from Eric's example
class CustomJSONEncoder(json.JSONEncoder):
    def default(self, obj):
        if isinstance(obj, datetime.datetime):
            return obj.isoformat()
        elif isinstance(obj, datetime.date):
            return obj.isoformat()
        elif isinstance(obj, decimal.Decimal):
            return str(obj)
        else:
            return super(CustomJSONEncoder, self).default(obj)

# Code taken from Eric's example
def json_str(value):
    return str(json.dumps(recursive_unicode(value), cls=CustomJSONEncoder).replace("</", "<\\/"))


class MainHandler(tornado.web.RequestHandler):
    
   @tornado.web.asynchronous 
   def post(self): 

      print ("Inside POST")

      ####################
      # Getting post data
      ####################
      data = json.loads(self.request.body)   

      file_name = ""
      try:
         image = str(data['image'])
         name = str(data['name'])
         count = int(data['count'])
         order = str(data['last'])
         
         file_name = base_image_path + name + str(count) + ".png"
         fo = open(file_name, "wb")
         png_image = bytes(base64.b64decode(image))
         fo.write(png_image)
         fo.close()
   
      except ValueError, e:
         print ("Problem Parsing Post " + str(e))
        
      ############################
      # Insert picture path in DB
      ############################
      print ("Inserting into Mongo")
      client = MongoClient() # localhost, default port
      collect = client.DroneRecognizer.ClassifierData
      collect.update({"name":name},
                     { "$push": {"images":file_name} }, 
                     upsert=True)      
    
      ###################
      # Sending response
      ###################
      self.set_header("Content-Type", "application/json")
      self.write(json_str({'arg1':"OK"}))
      
      ######################################################
      # Openning socket to let open cv the images are ready
      ######################################################
      if order == "true":
         print ("Making txt file")
         db_to_file()
         collect.remove({})

         try:
            sock = socket.socket()
            sock.connect((address, port))
            sock.send(base_path + 'database_contents.txt')
            sock.close()
            
         except socket.error, (value,message):
            print ("Problem Opening the socket or seding the data.")
            print (" ERROR " + str(message)) 

      #########################
      # Finish the asynch task
      #########################
      self.finish()

class DeleteHandler(tornado.web.RequestHandler):
   def post(self):
      print("Removing old pictures")

      remove_old_pictures(base_image_path)
      
      #####################
      # Sending response
      ####################
      self.set_header("Content-Type", "application/json")
      self.write(json_str({'arg1':"OK"}))
       

#########################################################
#
# Function that writes all the pictures' path to a file.
# The output file is called database_contents.txt
#
#########################################################
def db_to_file():
   client = MongoClient() # localhost, default port
   db = client.DroneRecognizer.ClassifierData

   # find all documents
   results = db.find()

   fo = open(base_path + 'database_contents.txt', 'w')

   ###########################################################################
   # Iterate through the documents and writing their paths in the output file
   ###########################################################################
   for item in results:
      for path in item['images']:
         fo.write(path + "\n")

   fo.close()   


################################################################
#
# Function that removes all the pictures in the pictures folder
# 
# Return: void
#
################################################################
def remove_old_pictures(path):

   for current_file in os.listdir(path):
      file_path = os.path.join(path, current_file)

      try:
         if (os.path.isfile(file_path)):
            os.unlink(file_path)

      except Exception, exception:
         print exception


#################################################
# Linking the different handelers to their paths
#################################################
application = tornado.web.Application([(r"/", MainHandler),
                                       (r"/remove", DeleteHandler),])

###############
#
#  Main Driver
#
###############
if __name__ == "__main__":
   application.listen(8888)
   tornado.ioloop.IOLoop.instance().start()
