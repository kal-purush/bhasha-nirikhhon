#!/usr/bin/python3
"""Alta3 Research | controlling pytest behaviors with conftest.py"""

import pytest
import requests

# functions and methods are called callable as they can be called.
# in python a line preceded by an @ sign is called a "decorator"
# think of  a package wrapped in a gift...
# the decorator doesn't alter the code on the inside
# but can provide it with some additional functionality
@pytest.fixture(autouse=True)
def disable_network_calls(monkeypatch):
    def stunted_get():
        # this is the error we want to raise if someone tries to call our target function
        raise RuntimeError("Network access not allowed during testing!")

    # creating a monkeypatch fixture will change the run behavior of any included functions
    # in the line below, the function to be altered is 'requests'
    # see https://docs.pytest.org/en/stable/ for more on using the pytest suite
    monkeypatch.setattr(requests, "get", lambda *args, **kwargs: stunted_get())