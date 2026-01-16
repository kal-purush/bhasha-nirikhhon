from unittest import TestCase
from src.problems.breakfastDrones import findMissingDrone


class BreakfastDronesTests(TestCase):
    def test_oneMissingDrone(self):
        delivery_id_confirmations = [1, 5, 5, 4, 4, 4, 4, 1, 3, 2, 2]
        result = findMissingDrone(delivery_id_confirmations)
        self.assertEqual(result, 3)

    def test_noUniqueKeys(self):
        delivery_id_confirmations = [1, 5, 5, 4, 4, 4, 4, 1, 3, 3, 2, 2]
        result = findMissingDrone(delivery_id_confirmations)
        self.assertIsNone(result)