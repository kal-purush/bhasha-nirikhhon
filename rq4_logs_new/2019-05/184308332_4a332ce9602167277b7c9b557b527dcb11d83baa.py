import unittest
import os
import sys
import json
import requests

sys.path.append("../")

from app import app, db

TEST_DB = "db.sqlite"


class test_pokemon(unittest.TestCase):
    @classmethod
    def setUp(self):
        app.config["TESTING"] = True
        app.config["WTF_CSRF_ENABLED"] = False
        app.config["DEBUG"] = False
        app.config["SQLALCHEMY_DATABASE_URI"] = "sqlite:///" + os.path.join(
            basedir, TEST_DB
        )
        self.app = app.test_client()
        db.create_all()

    @classmethod
    def tearDownClass(self):
        db.drop_all()

    def test_pokemon_post(self):
        print("_____________test pokemon post________________")
        input_data = {
            "pokemon": {
                "name": "charmamder",
                "sprite": "https://raw.githubusercontent.com/PokeAPI/sprites/master/sprites/pokemon/4.png",
                "cardColours": {"fg": "#eeeeee", "bg": "#3e3e3e", "desc": "#111111"},
            }
        }
        expected_output_data = {
            "pokemon": {
                "id": 1,
                "name": "charmander",
                "sprite": "https://raw.githubusercontent.com/PokeAPI/sprites/master/sprites/pokemon/4.png",
                "cardColours": {"fg": "#eeeeee", "bg": "#3e3e3e", "desc": "#111111"},
            }
        }
        actual_output_data = self.app.post(
            "http://localhost:8006/api/pokemon",
            data=json.dumps(input_data),
            content_type="application/json",
        )
        json_actual_output_data = json.loads(actual_output_data)
        self.assertEqual(json.dumps(input_data), json_actual_output_data)
        self.assertEqual(json_actual_output_data.status_code, 200)
        print("__________test pokemon post succesfull________")

    if __name__ == "__main__":
        unittest.main()
