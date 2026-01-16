#!/usr/bin/python3
"""
unit tests for base_model class
"""
from datetime import datetime
import inspect
import json
import unittest
import models
from models.base_model import BaseModel

class TestBaseModel(unittest.TestCase):
    """
    this will test the base_model class
    """

    @classmethod
    def setUpClass(cls):
        """
        drop all tables in db
        create new session
        """
        models.storage.drop_all()
        models.storage.reload()

    def test_create_no_kwargs(self):
        """
        check if BaseModel instance is created, no kwargs
        """
        new_base_model = BaseModel()
        self.assertIsInstance(new_base_model.id, str)
        self.assertIsNotNone(new_base_model.created_at)

    def test_create_kwargs(self):
        """
        check if BaseModel instance is created, kwargs
        """
        new_base_model = BaseModel(email="silver@", password="stef", age=12)
        self.assertIsInstance(new_base_model.email, str)
        self.assertIsInstance(new_base_model.age, int)
        self.assertIsInstance(new_base_model.password, str)
        self.assertNotEqual(new_base_model.password, "stef")

    def test_all_method(self):
        """
        check that all() returns dict
        """
        new_base_model = BaseModel()
        obj_dict = new_base_model.all()
        self.assertEqual(len(obj_dict), 0)
        self.assertIsInstance(new_base_model.all(), dict)

if __name__ == "__main__":
    unittest.main()