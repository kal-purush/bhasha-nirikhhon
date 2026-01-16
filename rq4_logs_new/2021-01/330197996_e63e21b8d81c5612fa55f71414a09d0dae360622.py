import requests
from flask import Flask, jsonify, request
import json
import time
import logging


logging.basicConfig(filename='./logs/web_log.log',
						filemode='a',
						format='%(asctime)s,%(msecs)d %(name)s %(levelname)s %(message)s',
						datefmt='%H:%M:%S',
						level=logging.DEBUG)

app = Flask(__name__)

  
#@app.route("/test", methods=["POST"])
#def test():
url = 'http://0.0.0.0:5052/api/'

#r1 = requests.post(url,'')
#print(r1, r1.text)
	  


data = "\n ***** Be happy & don't forget to smile Sana ^_^ !!\n"
j_data = json.dumps(data)
headers = {'content-type': 'application/json', 'Accept-Charset': 'UTF-8'}
for i in range(10):
   r = requests.post(url, data=j_data, headers=headers)
   logging.info(r.text)
	  
    

if __name__ == '__main__':
   app.run(debug=True, port=5000, host='0.0.0.0')
   
   