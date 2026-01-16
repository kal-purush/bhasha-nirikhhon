from flask import Flask, request
from flask import request
request.headers.get('your-header-name')
app = Flask(__name__)

@app.route('/', methods=['GET'])
def hello_world():
    return 'Hello, World'

@app.route("/accept")
def acceptRequest():
    print('processing request')
    #here we need to absorb the request status, body, headers

    print('forwarding request')
    # here we have to forward the request to the desired destination and wait for a response

    print('returning response')
    #here we need to absorb the request status, body, headers and send back

    return 'success'



if __name__ == "__main__":
    app.run(host="0.0.0.0", port=3000, debug=False)






# class Proxy:
#     # Class variables
#     request = {}
#     response = {}

#     def __init__(self, request, response):
#         self.request = request
#         self.response = response

#     def listen():
#         pass

#     def respond():
#         pass



# try:
#   toyProxy = Proxy()
# except:
#   print("An exception occurred")



