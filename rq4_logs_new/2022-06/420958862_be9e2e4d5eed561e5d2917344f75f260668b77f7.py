import os
import requests
import sys
import yaml


class Mirrors:
    def __init__(self, file: str) -> None:
        with open(file, 'r') as stream:
            try:
                self.list = yaml.safe_load(stream)
            except yaml.YAMLError as exc:
                print(exc)


class GiteaSettings:
    def __init__(self, gitea_token: str, gitea_owner: str, gitea_api_url: str, gitea_api_ssl: bool = True) -> None:
        self.token = gitea_token
        self.owner = gitea_owner
        self.api_url = gitea_api_url
        self.api_ssl = gitea_api_ssl
        self.api_migrate_url = f"{self.api_url}/repos/migrate"
        self.headers = {
            "Content-Type": "application/json",
            "Accept": "application/json",
            "Authorization": f"token {self.token}",
        }


class GitHubSettings:
    def __init__(self, github_pat: str, github_repos: list, gitea_conf: GiteaSettings) -> None:
        self.pat = github_pat
        self.repo_list = github_repos
        self.gitea_conf = gitea_conf

    def mirror(self, payload: dict) -> None:
        requests.post(
            url=self.gitea_conf.api_migrate_url,
            headers=self.gitea_conf.headers,
            verify=self.gitea_conf.api_ssl,
            json=payload,
        )

    def get_payload(self, repo_url: str) -> dict:
        # repo_name: turn "https://github.com/osism/release.git" into "release"
        payload = {
            "service": "github",
            "auth_token": self.pat,
            "clone_addr": repo_url,
            "mirror": True,
            "private": False,
            "repo_name": repo_url.split('/')[-1].split('.')[0],
            "repo_owner": self.gitea_conf.owner,
        }
        return payload

    def mirror_repos(self):
        for repo in self.repo_list:
            payload = self.get_payload("https://github.com/osism/release.git")
            self.mirror(payload=payload)


# Config file
mirrors = Mirrors(
    file=sys.argv[1],
)

# Gitea configuration
gitea_conf = GiteaSettings(
    gitea_token=os.environ.get("GITEA_TOKEN", ""),
    gitea_owner=os.environ.get("GITEA_OWNER", "osism"),
    gitea_api_url=os.environ.get("GITEA_API_URL", "https://gitea.services.osism.tech/api/v1"),
    gitea_api_ssl=os.environ.get("GITEA_API_SSL", True),
)

# GitHub mirroring
github_handle = GitHubSettings(
    github_pat=os.environ.get("GITHUB_PAT", ""),
    github_repos=mirrors.list['github'],
    gitea_conf=gitea_conf,
)
github_handle.mirror_repos()