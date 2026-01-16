"""
This helper class is intended to be used to randomly generate new diners for a restaurant Simulation
    - randomDinerGenerator() will randomly generate either a diner or return None
    - generateRandomName() will randomly generate a name (str) that is used for the random diner

* Note: if you have not installed the BeautifulSoup4 module in Pycharm, these methods cannot be used
    To install beautifulsoup4: go to Preferences (Mac) or Settings (PC), and under Project Interpreter,
    add and install beautifulsoup4
"""

import random
from Diner import Diner
from bs4 import BeautifulSoup
import urllib.request
import ssl


class RestaurantHelper(object):
    URL = "http://random-name-generator.info/"

    # Input: -
    # Return value: a new Diner object with a random name
    #             OR
    #             None (aka no new diner has arrived)
    @staticmethod
    def randomDinerGenerator():
        if random.randint(1, 10) % 3 == 0:
            return Diner(RestaurantHelper.generateRandName())
        else:
            return None

    # UNCOMMENT THIS IF DOING EC OPTION 2
    # also comment out the above definition of the method
    # @staticmethod
    # def randomDinerGenerator():
    #     if random.randint(1, 10) % 3 == 0:
    #         return Diner(RestaurantHelper.generateRandName(), random.randrange(3))
    #     else:
    #         return None

    # Input: -
    # Return: random name
    # DO NOT make any method calls to generateRandName
    @staticmethod
    def generateRandName():
        context = ssl._create_unverified_context()
        page = urllib.request.urlopen(RestaurantHelper.URL, context=context)
        soup = BeautifulSoup(page.read(), "html.parser")
        orderedListName = soup.find(class_="nameList")
        return orderedListName.find("li").text.split()[0]