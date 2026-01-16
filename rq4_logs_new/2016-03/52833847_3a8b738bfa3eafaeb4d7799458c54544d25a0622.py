from volunteer import Volunteer
from app import Session
import unittest
from datetime import datetime
from sqlalchemy import exc
# volunteer contains: name, email, passwordhash, phone, last_activity,
#			birthdate=None, about=None, gender=None,
#			vhours=None, neighborhood=None, interests=None, 
#			skills=None, education=None, availability=None, events=None

    class VolunteerTests(unittest.TestCase):

    #checks if the volunteer's fields are initialized correctly
    def test_init(self):
        joey = Volunteer('Joey Wood', 'wood.jos@husky.neu.edu', 'lit', '3015559721', '03/26/16 1:00', '05/26/1990',
                         'Snell rhymes with hell', 'Male', 0, "Back Bay", "Teaching", "Teaching", "College",
                         "Mondays @ 3pm - 6pm", "")
        self.assertTrue(joey.name == 'Joey Wood' and
                        joey.email == 'wood.jos@husky.neu.edu' and
                        joey.passwordhash == 'lit' and
                        joey.phone == '3015559721' and
                        joey.last_activity == '03/26/16' and
			joey.birthdate == '05/26/1990' and
                        joey.about == 'Snell rhymes with hell' and
                        joey.gender == 'Male' and
			joey.vhours == 0 and
                        joey.neighborhood == "Back bay" and
                        joey.interests == "Teaching" and 
			joey.skills == "Teaching" and
                        joey.education == "College" and
                        joey.availability == "Mondays @ 3pm - 6pm" and
                        joey.events == "")

    #test object write to the database.    
    def test_db_write(self):
        s = Session()
        try:
            s.add(joey)
            s.commit()
            s.close()
            self.assertTrue(True)
        except exc.SQLAlchemyError:
            self.assertTrue(False)


    # checks if the volunteer was added to the database after initialization
    def test_queryName(self):
        session = Session()
        joey = Volunteer('Joey Wood', 'wood.jos@husky.neu.edu', 'lit', '3015559721', '03/26/16 1:00', '05/26/1990',
                         'Snell rhymes with hell', 'Male', 0, "Back Bay", "Teaching", "Teaching", "College",
                         "Mondays @ 3pm - 6pm", "")         
        poey = session.query(Volunteer).filter_by(name='Joey Wood').first()
        self.assertTrue(joey.name == 'Joey Wood' and
                        joey.email == 'wood.jos@husky.neu.edu' and
                        joey.passwordhash == 'lit' and
                        joey.phone == '3015559721' and
                        joey.last_activity == '03/26/16' and
			joey.birthdate == '05/26/1990' and
                        joey.about == 'Snell rhymes with hell' and
                        joey.gender == 'Male' and
			joey.vhours == 0 and
                        joey.neighborhood == "Back bay" and
                        joey.interests == "Teaching" and 
			joey.skills == "Teaching" and
                        joey.education == "College" and
                        joey.availability == "Mondays @ 3pm - 6pm" and
                        joey.events == "")

    # checks if the volunteer can be queried by email
    def test_queryEmail(self):
        session = Session()
        joey = Volunteer('Joey Wood', 'wood.jos@husky.neu.edu', 'lit', '3015559721', '03/26/16 1:00', '05/26/1990',
                         'Snell rhymes with hell', 'Male', 0, "Back Bay", "Teaching", "Teaching", "College",
                         "Mondays @ 3pm - 6pm", "")         
        poey = session.query(Volunteer).filter_by(email='wood.jos@husky.neu.edu').first()
        self.assertTrue(joey.name == 'Joey Wood' and
                        joey.email == 'wood.jos@husky.neu.edu' and
                        joey.passwordhash == 'lit' and
                        joey.phone == '3015559721' and
                        joey.last_activity == '03/26/16' and
			joey.birthdate == '05/26/1990' and
                        joey.about == 'Snell rhymes with hell' and
                        joey.gender == 'Male' and
			joey.vhours == 0 and
                        joey.neighborhood == "Back bay" and
                        joey.interests == "Teaching" and 
			joey.skills == "Teaching" and
                        joey.education == "College" and
                        joey.availability == "Mondays @ 3pm - 6pm" and
                        joey.events == "")

    # checks if the volunteer can be queried by phone
    def test_queryPhone(self):
        session = Session()
        joey = Volunteer('Joey Wood', 'wood.jos@husky.neu.edu', 'lit', '3015559721', '03/26/16 1:00', '05/26/1990',
                         'Snell rhymes with hell', 'Male', 0, "Back Bay", "Teaching", "Teaching", "College",
                         "Mondays @ 3pm - 6pm", "")         
        poey = session.query(Volunteer).filter_by(phone='3015559721').first()
        self.assertTrue(joey.name == 'Joey Wood' and
                        joey.email == 'wood.jos@husky.neu.edu' and
                        joey.passwordhash == 'lit' and
                        joey.phone == '3015559721' and
                        joey.last_activity == '03/26/16' and
			joey.birthdate == '05/26/1990' and
                        joey.about == 'Snell rhymes with hell' and
                        joey.gender == 'Male' and
			joey.vhours == 0 and
                        joey.neighborhood == "Back bay" and
                        joey.interests == "Teaching" and 
			joey.skills == "Teaching" and
                        joey.education == "College" and
                        joey.availability == "Mondays @ 3pm - 6pm" and
                        joey.events == "")


    # # Email is valid
    # def test_phone_number_symbol(self):
    #    joey = Volunteer('Joey Wood', 'wood.jos@husky.neu.ed', 'lit', '301555972!', '03/26/16 1:00', '05/26/1990',
    #                     'Snell rhymes with hell', 'Male', 0, "Back Bay", "Teaching", "Teaching", "College",
    #                     "Mondays @ 3pm - 6pm", "")
    #     self.assertRaises(ValueError, 'Email must be vald')



    # # Phone is a string of 10 ints
    # def test_phone_number_symbol(self):
    #    joey = Volunteer('Joey Wood', 'wood.jos@husky.neu.edu', 'lit', '301555972!', '03/26/16 1:00', '05/26/1990',
    #                     'Snell rhymes with hell', 'Male', 0, "Back Bay", "Teaching", "Teaching", "College",
    #                     "Mondays @ 3pm - 6pm", "")
    #     self.assertRaises(ValueError, 'Phone numbers must be a string of 10 integers')

     # # Phone is a string of 10 ints
    # def test_phone_number<10(self):
    #    joey = Volunteer('Joey Wood', 'wood.jos@husky.neu.edu', 'lit', '301555972', '03/26/16 1:00', '05/26/1990',
    #                     'Snell rhymes with hell', 'Male', 0, "Back Bay", "Teaching", "Teaching", "College",
    #                     "Mondays @ 3pm - 6pm", "")
    #     self.assertRaises(ValueError, 'Phone numbers must be a string of 10 integers')

   # Phone is a string of 10 ints
    # def test_phone_number>10(self):
    #    joey = Volunteer('Joey Wood', 'wood.jos@husky.neu.edu', 'lit', '30155597211', '03/26/16 1:00', '05/26/1990',
    #                     'Snell rhymes with hell', 'Male', 0, "Back Bay", "Teaching", "Teaching", "College",
    #                     "Mondays @ 3pm - 6pm", "")
    #     self.assertRaises(ValueError, 'Phone numbers must be a string of 10 integers')

    # # joey.last_activity_is a string - should be in the form mm/dd/yyyy, hh:mm
    # def test_last_activity_format0(self):
    #    joey = Volunteer('Joey Wood', 'wood.jos@husky.neu.edu', 'lit', '3015559721', '03/26/16 1:00:00', '05/26/1990',
    #                     'Snell rhymes with hell', 'Male', 0, "Back Bay", "Teaching", "Teaching", "College",
    #                     "Mondays @ 3pm - 6pm", "")
    #     self.assertRaises(ValueError, 'last activity must be in the form mm/dd/yyyy hh:mm')

    # # joey.last_activity_is a string - should be in the form mm/dd/yyyy, hh:mm
    # def test_last_activity_format1(self):
    #     joey = Volunteer('Joey Wood', 'wood.jos@husky.neu.edu', 'lit', '3015559721', '03/26/16 1am', '05/26/1990',
    #                     'Snell rhymes with hell', 'Male', 0, "Back Bay", "Teaching", "Teaching", "College",
    #                     "Mondays @ 3pm - 6pm", "")
    #     self.assertRaises(ValueError, 'last activity must be in the form mm/dd/yyyy hh:mm')

    # # joey.last_activity_must be in the past
    # def test_last_activity_format1(self):
    #     joey = Volunteer('Joey Wood', 'wood.jos@husky.neu.edu', 'lit', '3015559721', '04/26/2020 1:00', '05/26/1990',
    #                     'Snell rhymes with hell', 'Male', 0, "Back Bay", "Teaching", "Teaching", "College",
    #                     "Mondays @ 3pm - 6pm", "")
    #     self.assertRaises(ValueError, 'last activity must be in the form mm/dd/yyyy hh:mm')

    # # joey.birthday is a string - should be in form mm/dd/yyyy
    # def test_birthday_format0(self):
    #     joey = Volunteer('Joey Wood', 'wood.jos@husky.neu.edu', 'lit', '3015559721', '03/26/16 1:00', '05/26/90',
    #                     'Snell rhymes with hell', 'Male', 0, "Back Bay", "Teaching", "Teaching", "College",
    #                     "Mondays @ 3pm - 6pm", "")
    #     self.assertRaises(ValueError, 'birthday must be in the form mm/dd/yyyy')

    # # joey.birthday is a string - should be in the form mm/dd/yyyy
    # def test_birthday_format1(self):
    #     joey = Volunteer('Joey Wood', 'wood.jos@husky.neu.edu', 'lit', '3015559721', '03/26/16 1:00', '5/26/1990',
    #                     'Snell rhymes with hell', 'Male', 0, "Back Bay", "Teaching", "Teaching", "College",
    #                     "Mondays @ 3pm - 6pm", "")
    #     self.assertRaises(ValueError, 'birthday must be in the form mm/dd/yyyy')

    # # joey.birthday is a string of letters - should be in the past
    # def test_birthday_past(self):
    #     joey = Volunteer('Joey Wood', 'wood.jos@husky.neu.edu', 'lit', '3015559721', '03/26/16 1:00', '05/26/2020',
    #                     'Snell rhymes with hell', 'Male', 0, "Back Bay", "Teaching", "Teaching", "College",
    #                     "Mondays @ 3pm - 6pm", "")
    #     self.assertRaises(ValueError, 'birthday must be in the past')


    # These tests require the Interest and Skills Enumerations to be created

    # # joey.interests should exist in the interests table
    # def test_interests_exists(self):
    #     session = Session()
    #     self.assertEqual(self.joey.interests, session.query(Interests).filter_by(name=self.joey.interests).first())
    #     session.close()
    #
    # # joey.skills should exist in the skills table
    # def test_skills_exists(self):
    #     session = Session()
    #     self.assertEqual(self.joey.skills, session.query(Skills).filter_by(name=self.joey.skills).first())
    #     session.close()



if __name__ == '__main__':
    unittest.main()




