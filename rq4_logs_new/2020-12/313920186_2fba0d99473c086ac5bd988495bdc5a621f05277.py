#!/usr/local/bin/python3
# -*- coding: utf-8 -*-
# Authors: Julien MARTIN-PRIN & Valentin FERNANDES

import unittest
from tournament import *

class TournamentTest(unittest.TestCase):
	
	def setUp(self):
		"""
		Init of the test cases
		"""
		self.tnmt = Tournament(10)
	
	def test_setup(self):
		"""
		Setup test
		"""
		player_list = self.tnmt.setup(1)
		self.assertEqual(len(player_list), 1)

	def test_newround(self):
		"""
		New round test
		"""

	def test_updaterank(self):
		"""
		Update rank test
		"""
	
	def test_ejectplayers(self):
		"""
		Eject player test
		"""

if __name__ == '__main__':
	unittest.main()