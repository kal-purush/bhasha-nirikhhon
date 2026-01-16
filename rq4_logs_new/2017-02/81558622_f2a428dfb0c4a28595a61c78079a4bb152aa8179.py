import os
import json

__author__ = 'Manuel Escriche'

home = os.path.dirname(os.path.abspath(__file__))

with open(os.path.join(home, 'entities.json')) as file:
    entities = json.load(file)

with open(os.path.join(home, 'owners.json')) as file:
    owners = json.load(file)

with open(os.path.join(home, 'sources.json')) as file:
    sources = json.load(file)

with open(os.path.join(home, 'enablers.json')) as file:
    enablers = json.load(file)

with open(os.path.join(home, 'metrics_endpoints.json')) as file:
    endpoints = json.load(file)