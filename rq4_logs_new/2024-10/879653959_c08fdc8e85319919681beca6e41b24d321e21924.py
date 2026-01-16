from flask import Flask, request, jsonify
import requests
from requests.auth import HTTPBasicAuth
import json

# Creating a flask app instance
app = Flask(__name__)



@app.route("/createJIRA", methods=['POST'])
def createJIRA():

    url = ""
    APITOKEN = ""

    auth = HTTPBasicAuth("hagek83409@advitize.com", APITOKEN)

    headers = {
    "Accept": "application/json",
    "Content-Type": "application/json"
    }

    data = request.get_json()

    # print(data.get("comment",{}).get("body"))
    comment_body = data.get("comment", {}).get("body")
    print("Comment body received:", comment_body) 

    if "jira" in data.get("comment",{}).get("body"):

        payload = json.dumps( {
        "fields": {

            "description": {
            "content": [
                {
                "content": [
                    {
                    "text": "My first jira ticket",
                    "type": "text"
                    }
                ],
                "type": "paragraph"
                }
            ],
            "type": "doc",
            "version": 1
            },

            "issuetype": {
            "id": "10012"
            },

            "project": {
            "key": "DEV"
            },
            "summary": "Updated jira ticket",
        },
        "update": {}
        } )

        response = requests.request("POST",url,data=payload,headers=headers,auth=auth)

        return json.dumps(json.loads(response.text), sort_keys=True, indent=4, separators=(",", ": ")) 
    else:
        return jsonify({"message": "No action taken. Comment does not contain 'jira'."}), 400


if __name__ == '__main__':
    app.run(host='0.0.0.0', port=5000)