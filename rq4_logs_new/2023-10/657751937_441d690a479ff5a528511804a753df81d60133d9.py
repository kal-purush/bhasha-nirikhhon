from osprey.client import all_sources
from osprey.client import create_source

def test_list_sources():
    # when database is not populated, assert empty
    assert all_sources() == []

    # add data and assert that it is not empty
    name = 'test-1'
    url = 'https://dummyjson.com/products/1'
    create_source(name=name, url=url)
    exp_resp = [{'description': None, 'id': 1, 'modifier': None, 'name': name, 'timer': 86400, 'url': url, 'verifier': None}]
    assert all_sources() == exp_resp

def test_create_source():
    pass

def test_get_file():
    pass

def test_list_proxies():
    pass