from event import Event
from app import Session
import unittest
from organization import Organization
from datetime import datetime
# event contains: id, name, address, city, state, zip, about, start_at, posted_at, duration, interests, skills, org
class EventTests(unittest.TestCase):
    race = Event(1, 'Race for the cure', 'Mass Ave', 'Boston', 'MA', '02115',
                   'Running a marathon to raise money for cancer research', '04/02/2016 13:00', '03/27/2016 24:00:00',
                   2, 'cancer', 'running', 'Race for the Cure')
    #checks if the events fields are initialized correctly
    def test_init(self):
        self.assertTrue(self.race.id == 1 and
                        self.race.name == 'Race for the cure' and
                        self.race.address == 'Mass Ave' and
                        self.race.city == 'Boston' and
                        self.race.state == 'MA' and
                        self.race.zip == '02115' and
                        self.race.about == 'Running a marathon to raise money for cancer research' and
                        self.race.start_at == '04/02/2016 13:00' and
                        self.race.posted_at == '03/27/2016 24:00:00' and
                        self.race.duration == 2 and
                        self.race.interests == 'cancer' and
                        self.race.skills == 'running' and
                        self.race.org == 'Race for the Cure')

    # checks if the event was added to the database after initialization
    def test_query(self):
        session = Session()
        self.assertEqual(session.query(Event).filter_by(id=1).first(), self.race)

    # race.zip is a string of letters - should be 5 ints
    def test_zip_letters(self):
        race = Event(1, 'Race for the cure', 'Mass Ave', 'Boston', 'MA', 'abcdef',
                   'Running a marathon to raise money for cancer research', '04/02/2016 13:00', '03/27/2016 24:00:00',
                   2, 'cancer', 'running', 'Race for the Cure')
        self.assertRaises(ValueError, 'zip-codes must be a string of 5 integers')

    # race.zip is too long - should be 5 ints
    def test_zip_length(self):
        race = Event(1, 'Race for the cure', 'Mass Ave', 'Boston', 'MA', '123456789',
                   'Running a marathon to raise money for cancer research', '04/02/2016 13:00', '03/27/2016 24:00:00',
                   2, 'cancer', 'running', 'Race for the Cure')
        self.assertRaises(ValueError, 'zip-codes must be a string of 5 integers')

    # race.start_at is a string of letters - should be in the form mm/dd/yyyy, hh:mm
    def test_startAt_letters(self):
        race = Event(1, 'Race for the cure', 'Mass Ave', 'Boston', 'MA', '02115',
                   'Running a marathon to raise money for cancer research', 'oops', '03/27/2016 24:00:00',
                   2, 'cancer', 'running', 'Race for the Cure')
        self.assertRaises(ValueError, 'start time must be in the form mm/dd/yyyy hh:mm')

    # race.start_at is improperly formatted - should be in the form mm/dd/yyyy, hh:mm
    def test_startAt_format(self):
        race = Event(1, 'Race for the cure', 'Mass Ave', 'Boston', 'MA', '02115',
                   'Running a marathon to raise money for cancer research', 'April 12, 2016 1pm', '03/27/2016 24:00:00',
                   2, 'cancer', 'running', 'Race for the Cure')
        self.assertRaises(ValueError, 'start time must be in the form mm/dd/yyyy hh:mm')

    # race.start_at cannot be in the past
    def test_startAt_past(self):
        race = Event(1, 'Race for the cure', 'Mass Ave', 'Boston', 'MA', '02115',
                   'Running a marathon to raise money for cancer research', 'April 12, 2014 1pm', '03/27/2016 24:00:00',
                   2, 'cancer', 'running', 'Race for the Cure')
        self.assertRaises(ValueError, 'start time must be in the form mm/dd/yyyy hh:mm')

    # race.posted_at is a string of letters - should be in the form mm/dd/yyyy, hh:mm:ss
    def test_postedAt_letters(self):
        race = Event(1, 'Race for the cure', 'Mass Ave', 'Boston', 'MA', '02115',
                   'Running a marathon to raise money for cancer research', '04/02/2016 13:00', 'oops',
                   2, 'cancer', 'running', 'Race for the Cure')
        self.assertRaises(ValueError, 'post time must be in the form mm/dd/yyyy hh:mm:ss')

    # race.posted_at is improperly formatted - should be in the form mm/dd/yyyy, hh:mm:ss
    def test_postedAt_format(self):
        race = Event(1, 'Race for the cure', 'Mass Ave', 'Boston', 'MA', '02115',
                   'Running a marathon to raise money for cancer research', '04/02/2016 13:00', 'March 27, 2016 12am',
                   2, 'cancer', 'running', 'Race for the Cure')
        self.assertRaises(ValueError, 'post time must be in the form mm/dd/yyyy hh:mm:ss')

    # These tests require the Interest and Skills Enumerations to be created

    # # race.interests should exist in the interests table
    # def test_interests_exists(self):
    #     session = Session()
    #     self.assertEqual(self.race.interests, session.query(Interests).filter_by(name=self.race.interests).first())
    #     session.close()
    #
    # # race.skills should exist in the skills table
    # def test_skills_exists(self):
    #     session = Session()
    #     self.assertEqual(self.race.skills, session.query(Skills).filter_by(name=self.race.skills).first())
    #     session.close()

     # race.org should exist in the skills table
    def test_org_exists(self):
        session = Session()
        self.assertEqual(self.race.org, session.query(Organization).filter_by(id=self.race.org.id).first())
        session.close()

if __name__ == '__main__':
    unittest.main()




