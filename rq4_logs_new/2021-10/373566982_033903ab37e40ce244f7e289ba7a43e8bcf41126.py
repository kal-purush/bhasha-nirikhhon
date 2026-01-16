"""
    End to End Tests
"""

import parse_topo
import pytest
from tests import helper
import requests
import os
import time
import json


class TestE2E:
    """Class designed to contain all the E2E tests pertaining to the
     Amlight-SDx napp to verify all its functionalities"""

    def end_to_end_test_1_1(self):
        """Tests that the topology is up, and a basic schema is generated"""
        pass

    def end_to_end_test_1_2(self):
        """ Tests that the timestamp is properly updated after a Kytos event
        was registered"""
        pass

    def end_to_end_test_1_3(self):
        """Tests that the version is properly updated after an operational event
        was registered"""
        pass

    def end_to_end_test_1_4(self):
        """Test that the topology name was successfully added to the schema's 'id'
         attribute after been provided by an admin"""
        pass

    def end_to_end_test_1_5(self):
        """Test that the list of dictionaries for Links IS NOT empty upon napp
         initialization"""
        pass

    def end_to_end_test_1_6(self):
        """Test that the list of dictionaries for Switches IS NOT empty upon napp
         initialization"""
        pass