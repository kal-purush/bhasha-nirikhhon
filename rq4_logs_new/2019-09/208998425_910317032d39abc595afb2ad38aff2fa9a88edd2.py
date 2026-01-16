# from .settings import settings
import requests
# from . import util

client_id = '723530674332-g9bq3lq6e921pip895vk6mgg91fhui4l.apps.googleusercontent.com'
redirect_uri = 'https://kyukou.shosato.jp/oauth/google/redirect'
scope = 'email profile openid'
access_type = 'offline'
state = 'hogehoge'  # util.generate_id(40)
prompt = 'select_account'
response_type = 'code'


def get_redirect_link():
    return 'https://accounts.google.com/o/oauth2/v2/auth'
    + f"client_id={client_id}&"
    + f"include_granted_scopes={'true'}&"
    + f"redirect_uri={redirect_uri}&"
    + f"scope={scope}&"
    + f"access_type={access_type}&"
    + f"state={state}&"
    + f"prompt={prompt}&"
    + f"response_type={response_type}"