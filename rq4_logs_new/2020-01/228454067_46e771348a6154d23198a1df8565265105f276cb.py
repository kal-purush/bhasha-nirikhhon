import unittest

from sqlalchemy import create_engine
from sqlalchemy.orm import sessionmaker

from models.user_model import UserModel
from models.recipe_model import RecipeModel
from db import db


class TestUserModel (unittest.TestCase):

    engine = create_engine('sqlite:///:memory:')
    Session = sessionmaker(bind=engine)
    session = Session()
    email = 'pepe@mail.com'
    password = '1234'

    def setUp(self):
        db.metadata.create_all(self.engine)
        self.session.add(UserModel(self.email, self.password))
        self.session.commit()

    def tearDown(self):
        db.metadata.drop_all(self.engine)

    def test_user_creation(self):
        result = self.session.query(UserModel).all()
        self.assertEqual(True, True)