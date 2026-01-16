import jwt
from rest_framework.exceptions import AuthenticationFailed
from django.conf import settings


def get_user_id_from_token(token):
    try:
        decoded_token = jwt.decode(token, settings.SECRET_KEY, algorithms=['HS256'])
    except jwt.exceptions.ExpiredSignatureError:
        raise AuthenticationFailed('토큰이 만료되었습니다')
    except jwt.exceptions.DecodeError:
        raise AuthenticationFailed('토큰이 유효하지 않습니다')
    user_id = decoded_token['user_id']
    return user_id