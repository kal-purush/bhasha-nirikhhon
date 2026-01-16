"""
To use this plugin, create githubconfig.json in the base directory (same directory as bot.py)
githubconfig.json format:
{
    "owner": "repo_owner",
    "repo": "repo_name",
    "branch": "config_branch", (optional, defaults to "master")
    "config_path": "skybot_config_path", (optional, defaults to "config.json")
    "private_token": "github_token_for_user_with_repo_read_access"
}
If enabled, this plugin will execute on init/reload, overwriting config.json.
"""
import base64
import json
import os
import urllib2
from urllib2 import HTTPError

from util import hook, http

GITHUB_CONFIG = {}
LAST_COMMIT_HASH = ''

REPO_API_PATH = 'https://api.github.com/repos'

CONFIG_PATH_TEMPLATE = REPO_API_PATH + '/{owner}/{repo}/contents/{config_path}'
LAST_COMMIT_PATH_TEMPLATE = REPO_API_PATH + '/{owner}/{repo}/git/refs/heads/{branch}'

@hook.command('refreshconfig')
def githubconfig(inp, db=None):
    load_github_config() # reload GITHUB_CONFIG from githubconfig.json

    global GITHUB_CONFIG
    required_keys = ['owner', 'repo', 'private_token']

    if not GITHUB_CONFIG or GITHUB_CONFIG is None:
        return "Missing GitHub config."

    for required_key in required_keys:
        if required_key not in GITHUB_CONFIG:
            return "Missing API key '" + required_key + "'."

    if 'branch' not in GITHUB_CONFIG:
        GITHUB_CONFIG['branch'] = 'master' # default to master branch
    if 'config_path' not in GITHUB_CONFIG:
        GITHUB_CONFIG['config_path'] = 'config.json' # default to config.json in base directory

    global LAST_COMMIT_HASH
    last_commit = get_last_commit(GITHUB_CONFIG)

    if LAST_COMMIT_HASH == last_commit:
        return "Stored commit hash matches latest commit. No changes have been made."
    else:
        LAST_COMMIT_HASH = last_commit

    config_str = fetch_remote_config(GITHUB_CONFIG)

    open('config.json', 'w').write(config_str)
    return "Updated config from remote repo."

def get_last_commit(github_config):
    url = LAST_COMMIT_PATH_TEMPLATE.format(**github_config)
    response = fetch_json(url, github_config)
    if response is not None:
        return response['object']['sha']
    return None

def fetch_remote_config(github_config):
    url = CONFIG_PATH_TEMPLATE.format(**github_config)
    response = fetch_json(url, github_config)
    if response is not None:
        return base64.b64decode(response['content'])
    return None

def fetch_json(url, github_config):
    auth_headers = {
        "Authorization": "token " + github_config['private_token']
    }
    req = urllib2.Request(url, headers=auth_headers)
    try:
        urllib2.urlopen(req)
        return json.loads(urllib2.urlopen(req).read())
    except HTTPError, e:
        print "Error getting from URL", url, e.getcode()
        return None
    except StandardError, e:
        print "Unknown error", e
        return None
    return None

def load_github_config():
    global GITHUB_CONFIG
    github_config_path = 'githubconfig.json'
    if os.path.exists(github_config_path):
        try:
            GITHUB_CONFIG = json.load(open(github_config_path, 'r'))
        except ValueError:
            return "Error parsing " + github_config_path
            GITHUB_CONFIG = None
    else:
        return github_config_path + " not found"
        GITHUB_CONFIG = None
    return "Successfully parsed " + github_config_path

# This should execute on init/reload
print load_github_config()
if GITHUB_CONFIG:
    print githubconfig(None)