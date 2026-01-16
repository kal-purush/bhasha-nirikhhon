from dataclasses import dataclass

import requests


@dataclass
class GithubUserData:
    login: str
    avatar_url: str


@dataclass
class GithubClient:
    """
    Returns None, when request is not successfull.
    """

    access_token: str

    def get_user_data(self):
        r = requests.get(
            "https://api.github.com/user",
            headers={'Authorization': 'token {}'.format(self.access_token)},
        )
        if r.status_code != 200:
            return None
        else:
            return GithubUserData(
                login=r.json()["login"],
                avatar_url=r.json()["avatar_url"],
            )