from typing import Optional
from datetime import datetime as dt
import os
import requests
import logging

from jwt import decode, ExpiredSignatureError
from dotenv import load_dotenv

log = logging.getLogger(__name__)

load_dotenv()

user_agent = " / ".join(
    os.environ[k] for k in ("CLIENT_APP", "DISCORD_USERNAME", "CONTACT_EMAIL")
)
base_url_d = {
    "NadeoServices": "https://prod.trackmania.core.nadeo.online",
    "NadeoLiveServices": "https://live-services.trackmania.nadeo.live/api/token",
    "OAuth": "https://api.trackmania.com/api",
}


class NadeoCredentials:
    base_headers: dict = {
        "Content-Type": "application/json",
        "User-Agent": f"{user_agent}",
    }
    refreshable: bool = True

    def __init__(
        self,
        audience: str = "NadeoServices",
    ):
        self.audience = audience
        self.base_url = base_url_d[audience]
        self.tokens: dict[str, Optional[str]] = {"access": None, "refresh": None}
        self._init_tokens()

    def _init_tokens(self) -> None:
        """Validates the access and refresh tokens, refreshing if necessary."""
        refresh = False
        try:
            self.tokens["access"] = self._load("access")
            try:
                self._decoded("access")
                return
            except ExpiredSignatureError:
                log.info(f"{self.audience} access token has expired.")
                if self.refreshable:
                    self.tokens["refresh"] = self._load("refresh")
                    try:
                        self._decoded("refresh")
                    except ExpiredSignatureError:
                        log.info(f"{self.audience} refresh token has expired.")
                    else:
                        refresh = True
        except FileNotFoundError:
            log.info(f"No {self.audience} access token found.")
        self.tokens = self._update_tokens(refresh)
        for name, token in self.tokens.items():
            log.info(f"New {self.audience} {name} token expires at {self._exp(name)}.")
            self._save(name)

    def _update_tokens(self, refresh: bool = False) -> dict:
        """Gets a token pair for the given audience, refreshing if necessary.

        :param refresh: If True, refreshes the access token using the refresh token.
        :return: dict with keys "access" and "refresh" and corresponding token strings.
        """
        ubisoft_prod = f"{base_url_d['NadeoServices']}/v2/authentication/token"
        if not refresh:
            token_url = f"{ubisoft_prod}/ubiservices"
            auth = f"ubi_v1 t={ubisoft_token_auth()}"
        else:
            token_url = f"{ubisoft_prod}/refresh"
            auth = f"nadeo_v1 t={self.tokens['refresh']}"
        response = requests.post(
            url=token_url,
            headers=self.request_headers(auth),
            json={"audience": self.audience},
        )
        assert_valid_response(response)
        tokens = response.json()
        if refresh:
            log.info(f"Refreshed {self.audience} access token.")
        return {
            "access": tokens["accessToken"],
            "refresh": tokens["refreshToken"],
        }

    def request_headers(self, auth: Optional[str] = None) -> dict:
        """Returns the headers for a request to the given audience, with header["Authorization"] = auth."""
        return dict(self.base_headers, **{"Authorization": auth})

    def _load(self, name: str) -> str:
        with open(f"tokens/{name}_token_{self.audience}.txt", "r") as f:
            return f.read()

    def _save(self, name: str) -> None:
        with open(f"tokens/{name}_token_{self.audience}.txt", "w") as f:
            f.write(self.tokens[name])

    def _decoded(self, name: Optional[str]) -> dict:
        return decode(
            self.tokens[name],
            options={
                "verify_signature": False,
                "verify_exp": True,
                "verify_iat": True,
                "verify_nbf": True,
            },
        )

    def _exp(self, name: str) -> dt:
        return dt.fromtimestamp(self._decoded(name)["exp"])


def assert_valid_response(response: requests.Response) -> None:
    """Asserts that the response gives a 200 status code."""
    assert response.status_code == 200, f"Request failed: {response.text}"


def ubisoft_token_auth() -> str:
    """Gets Ubisoft ticket and returns the corresponding encoded JWT string."""
    headers = dict(
        NadeoCredentials.base_headers,
        **{"Ubi-AppId": "86263886-327a-4328-ac69-527f0d20a237"},
    )
    ticket_response = requests.post(
        url="https://public-ubiservices.ubi.com/v3/profiles/sessions",
        headers=headers,
        auth=(os.environ["UBI_USERNAME"], os.environ["UBI_PASSWORD"]),
    )
    assert_valid_response(ticket_response)
    return ticket_response.json()["ticket"]