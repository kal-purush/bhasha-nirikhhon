"""
This file contains all functions that are used exclusively as controllers for different views.
"""

from conrec.foursquare_module import radius


def test_controller():
    return radius(455.254, 19.824, 500)