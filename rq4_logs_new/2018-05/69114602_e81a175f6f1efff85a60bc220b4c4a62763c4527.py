"""Simple command line student manager"""
students = []


def print_all_students():
    for student in students:
        print(student)


def print_first_three_student_number_digits():
    for student in students:
        print(str(student["student_number"])[:-3])


def add_student(name, student_number=999999):
    student = {
        "name": name,
        "student_number": student_number
    }
    students.append(student)

add_student("MalsR", 155144)
add_student(name="Gimley", student_number=155166)

print_all_students()
print_first_three_student_number_digits()