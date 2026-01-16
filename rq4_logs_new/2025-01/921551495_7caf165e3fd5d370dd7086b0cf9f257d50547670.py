class Student(object):
  student_count=0

def __new__(self):
  print('Student.__new__')

def __init__(self):
  print('student.__init__')
  self.gpa = 4.00

def study_hard(self):
  print('sir,yes sir.')