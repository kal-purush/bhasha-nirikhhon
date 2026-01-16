from auth.models import User
from auth.services import exceptions
import logging
import random
import crypt
import string
import json

# Get logger from django config.
defLogger = logging.getLogger('platform')
reqLogger = logging.getLogger('request')


def localUserCNTLR(userName, password):
    if userName is None or password is None:
        raise TypeError('please check your input params')
    try:
        user = User.objects.get(userName=userName)
        userAuth = LocalAuth.objects.get(uid=user.id)
        if userAuth.password == crypt.crypt(password, userAuth.salt):
            status = 0
            loaclUserMsg = 'localAuth success'
        else:
            raise exceptions.UserAuthFailed
    except User.DoesNotExist, e:
        loaclUserMsg = "User (\'{0}\') does not exist.".format(userName)
        print exceptions
        raise exceptions.UserDoesNotExist(loaclUserMsg)
    except Exception, e:
        loaclUserMsg = ("An unknown error occurred "
                        "while getting user {0}: {1}".format(userName, str(e)))
        raise exceptions.UnknownError(loaclUserMsg)
    return {'status': status,
            'msg': loaclUserMsg,
            'result': ''
            }


def genSalt(chars=string.letters + string.digits):
    return random.choice(chars) + random.choice(chars)

def localUserInit(uid):
    try:
        salt = genSalt()
        password = crypt.crypt('123456', salt)
        initHandler = LocalAuth(uid=uid,
                  salt=salt,
                  password=password,
                  )
        initHandler.save()
        status = 0
        loaclUserMsg = 'init user password success'
    except Exception, e:
        status = 1023
        loaclUserMsg = str(e)
    return {'status': status,
            'msg': loaclUserMsg,
            'result': ''
            }