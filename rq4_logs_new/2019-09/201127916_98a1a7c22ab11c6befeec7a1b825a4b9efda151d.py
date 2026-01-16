from flask import jsonify, request, make_response
from flask_restplus.namespace import RequestParser, request
from ....resources.admin import AdminManagement
from flask_jwt_extended import (create_access_token, create_refresh_token)
from ....resources.mail.reset_password import send_reset_password_email

#################
# Parser        #
#################
Parser = RequestParser()
Parser.add_argument('email',
                    help="The user's email",
                    required=True,
                    location='form')

#################
# Method        #
#################


def Post(args):
    email = args['email']

    am = AdminManagement()
    result, accessCode = am.request_reset(email)
    print("I received a {} and a {}".format(result, accessCode))
    if result == 1:  # Successfully requested
        send_reset_password_email(accessCode, email)
        response = jsonify({
            "statusCode":
            200,
            "message":
            "Successfully requested the password reset."
        })
    elif result == 0:  # Could not request it
        response = jsonify({
            "statusCode": 400,
            "message": "The access code is wrong.",
        })
    elif result == -1:  # User does not exist
        response = jsonify({
            "statusCode": 400,
            "message": "The account does not exist.",
        })
    elif result == -2:  # User is disabled
        response = jsonify({
            "statusCode":
            400,
            "message":
            "The account was disabled. Contact an administrator."
        })
    elif result == -3:  # User already has a reset token active
        response = jsonify({
            "statusCode":
            400,
            "message":
            "The account has already requested a password reset."
        })
    else:  #Unexpected response
        response = jsonify({
            "statusCode": 400,
            "message": "Something went wrong",
        })

    return response