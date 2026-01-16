import unittest
from tests.basetest import BaseTestCase
from app.Model import Base, Allocations, People, Rooms
import os
from sqlalchemy import create_engine, MetaData


class TestAmityDatabaseFunctions(BaseTestCase):
    def setUp(self):
        super(TestAmityDatabaseFunctions, self).setUp()
        self.engine = create_engine("sqlite:///test_database.db")
        Base.metadata.create_all(self.engine)

    def tearDown(self):
        db_path = os.path.dirname(os.path.realpath(__file__)) + "/../"
        complete_db_path = os.path.join(db_path, "test_database.db")
        os.remove(complete_db_path)

    def test_resets_database(self):
        self.amity.reset_db("test_database.db")

        assert not self.engine.dialect.has_table(self.engine, "rooms")

    def test_checks_for_existing_database(self):
        assert self.amity.check_db_exists("test_database.db")

    def test_connects_to_database(self):
        session = self.amity.connect_to_db("test_database.db")
        person = People(person_name="Joe", person_type="Fellows")
        session.add(person)
        session.commit()
        # Check was added
        person = session.query(People.person_name).first()

        self.assertTupleEqual(person, ("Joe", ))

    # Test saves state to database

    # Test loads state from database

    # Test does not create a database with a reserved name

if __name__ == "__main__":
    unittest.main()