import requests


def notify_me(message, config):
    requests.post(config['url'], json={'message': message})