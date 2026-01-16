import requests
from flask import Flask, request

UNSPLASH_URL='https://api.unsplash.com/photos/random'
UNSPLASH_KEY="wfwiDJRSvou-SR5O8bUrbBzkDM2SbPNedptkNBEF2uw"


application = Flask(__name__)
#in python, constants are uppercase.

@application.route("/new-image")
def new_image():
  #python convention uses underscores.
  word = request.args.get("query")

  headers = {
    "Accept-Version": "v1",
    "Authorization": "Client-ID " + UNSPLASH_KEY,
  }
  parameters = {
    "query": word
  }
  response = requests.get(url=UNSPLASH_URL, headers=headers, params=parameters)
  data = response.json()
  return {"data": data}


if __name__ == "__main__":
  application.run(host="0.0.0.0", port=5050)