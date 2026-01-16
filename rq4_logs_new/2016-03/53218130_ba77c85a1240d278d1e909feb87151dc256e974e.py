import unittest
from amity import Amity


class AmityTest(unittest.TestCase):

    def setUp(self):
        self.building = Amity()

    def test_populate_offices(self):
        '''
        Test whether the populate method
        populates the building with office rooms
        '''
        self.building.populate()
        self.assertTrue(len(self.building.office_rooms) > 0)

    def test_populate_living(self):
        '''
        Test whether the populate method
        populates the building with living rooms
        '''
        self.building.populate()
        self.assertTrue(len(self.building.living_rooms) > 0)

    def test_create_staff(self):
        '''
        Test create_occupants method
        '''
        self.assertTrue(len(self.building.staff))

    def test_create_fellows(self):
        '''
        Test create_occupants method
        '''
        self.building.create_occupants()
        self.assertTrue(
            len(self.building.boarding_fellows +
                self.building.non_boarding_fellows) > 0)

    def test_allocate(self):
        '''
        Tests allocate method
        '''
        self.building.allocate()
        self.assertTrue(len(self.building.get_allocations()) > 0)

    def test_get_available_room(self):
        '''
        Tests getting available rooms
        '''
        self.assertIsInstance(
            self.building.get_available_rooms('LIVING'), list)

    def test_get_unallocated(self):
        '''
        Tests getting unallocated staff
        '''
        self.assertIsInstance(
            self.building.get_unallocated(), list)

    # def test_print_allocations(self):
    #     '''
    #     Tests printing allocations
    #     '''
