#!/usr/bin/env python
# encoding: utf-8
"""
Vanilla requests style.
"""
import requests

with open('example.pdf', mode='rb') as fh:
    data = fh.read()

meta = requests.put('http://localhost:9998/meta', data, headers={'Accept': 'application/json'}).json()
content = requests.put('http://localhost:9998/tika', data, headers={'Content-Type': meta['Content-Type'], 'Accept-Encoding': 'utf-8'}).content

print({
    'meta': meta,
    'content': content
})