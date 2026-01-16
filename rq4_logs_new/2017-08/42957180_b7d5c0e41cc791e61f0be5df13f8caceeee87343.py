import webbrowser
import socket
import re


def get_access_token(client_id, env="devapi"):

    # used to run web server for capturing uri redirect
    HOST, PORT = '', 8081

    # fire up web server
    listen_socket = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    listen_socket.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
    listen_socket.bind((HOST, PORT))
    listen_socket.listen(1)

    # build required urls
    authorize_url = "https://%s.optionscity.com/oauth/authorize" % env
    callback_uri = "http://localhost:%s" % str(PORT)
    authorization_redirect_url = authorize_url + '?response_type=token&client_id=' + client_id + '&redirect_uri=' + callback_uri + '&scope=openid'

    # open browser with url and uri redirect
    webbrowser.open(authorization_redirect_url)

    # once access_token resolved, kill server and return good token
    access_token = None

    # loop looks for requests
    while True:

        # accept and read data
        client_connection, client_address = listen_socket.accept()
        request = client_connection.recv(1024)

        # parse request type and path
        match_obj = re.match(r'(GET) (\/.*) HTTP', request)
        request_type = match_obj.group(1)
        path = match_obj.group(2)

        # html page simply gets access_token from api and makes ajax request with the good token query params
        if path == '/' and request_type == 'GET':
            with open('citytrader/implicit-redirect.html', 'r') as htmlFile:
                response_status = 'HTTP/1.1 200 OK'
                response_body = htmlFile.read()

        # handle response with access token in path
        elif '?access_token' in path and request_type == 'GET':
            access_token = path.split("&")[0].split("=")[1]
            response_status = 'HTTP/1.1 200 OK'

        # handle any unexpected requests
        else:
            response_status ='HTTP/1.1 404 NOT FOUND'
            response_body = '404: NOT FOUND'

        # send response back to browser
        http_response = response_status + '\n\n' + response_body
        client_connection.sendall(http_response)
        client_connection.close()

        # if fully resolved access_token, break out of loop and return token
        # TODO can we somehow close tab from python?
        if access_token:
            listen_socket.close()
            return access_token
