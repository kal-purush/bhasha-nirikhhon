import unittest
from content import attendance_register

class Test_content(unittest.TestCase):
	def test_student_add(self):
		self.assertEqual(attendance_register("Joy Kendi", 1, False, 0), ("Joy Kendi", 1, False,0))
	def test_student_list(self):
		self.assertTrue(attendance_register("Joy Kendi",1, False, 0))
	def test_student_remove(self):
		self.assertFalse(attendance_register(1, ("Joy Kendi", 1, False,0)))
	def test_check_in(self):
		self.assertFalse(attendance_register("Joy Kendi, 1, False, 0"), "Joy Kendi, 1, False, 1")

if __name__=='__main__':
	unittest.main()