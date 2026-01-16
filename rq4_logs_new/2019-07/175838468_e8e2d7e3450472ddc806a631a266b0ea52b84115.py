from urllib import parse
from json.decoder import JSONDecodeError

import requests


class AuthException(Exception):
    pass


def authorize_url(authorize_url, client_id, redirect_uri, scope, state):
    return "{authorize_url}?client_id={client_id}&redirect_uri={redirect_uri}&scope={scope}&state={state}".format(
        authorize_url=authorize_url,
        client_id=client_id,
        redirect_uri=redirect_uri,
        scope=scope,
        state=state,
    )


def request_access_token(
    access_token_url, client_id, client_secret, code, redirect_uri, state
):
    r = requests.post(
        access_token_url,
        data=dict(
            client_id=client_id,
            client_secret=client_secret,
            code=code,
            redirect_uri=redirect_uri,
            state=state,
        ),
    )

    if r.status_code != 200:
        raise AuthException(r.text)

    resp = dict(parse.parse_qs(r.text))

    if "error" in resp:
        raise AuthException(resp["error"][0])

    if not resp:
        raise AuthException("There is not valid access token in the response")

    return resp


def get_user_details(user_resource_url, access_token):
    r = requests.get(
        user_resource_url, headers={"Authorization": "token {}".format(access_token)}
    )

    if r.status_code != 200:
        raise AuthException(r.text)

    try:
        return r.json()
    except JSONDecodeError:
        raise AuthException("There is a problem while parsing the response")