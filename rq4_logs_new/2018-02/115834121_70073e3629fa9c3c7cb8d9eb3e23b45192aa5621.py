#! /usr/bin/python

students = []

# parent class
class Student:

    # class attribute
    school_name = "Springfield Elementary"

    # initialization function
    def __init__(self, name, student_id=332):
        # set instance attributes
        self.name = name
        self.student_id = student_id
        students.append(self)

    # override __str__ to change what print() returns
    def __str__(self):
        return "Student {0}".format(self.name)

    # return capitalized name
    def get_name_capitalize(self):
        return self.name.capitalize()

    def get_school_name(self):
        # you still need 'self' here for class attributes 
        return self.school_name

# inherit from 'Student'
class HighSchoolStudent(Student):
    # override this class attribute
    school_name = "Springfield High School"

    # override get_school_name method
    def get_school_name(self):
        return "This is a high school student"

    # override but still run parent method
    def get_name_capitalize(self):
        # super() looks through parent classes!!!
        original_value = super().get_name_capitalize()
        return original_value + "-HS"

james = HighSchoolStudent("james")
print(james.get_name_capitalize())
print(james.get_school_name())
