import logging
import random
from sqlalchemy import create_engine, Column, Integer, String, ForeignKey
from sqlalchemy.orm import declarative_base as declarative
from sqlalchemy.orm import sessionmaker, relationship

# Create a logger
logger = logging.getLogger(__name__)

# Set the logging level to DEBUG
logger.setLevel(logging.DEBUG)

# Create a file handler which logs even debug messages
file_handler = logging.FileHandler('app.log', mode='w')
file_handler.setLevel(logging.DEBUG)

# Create a console handler with a higher log level
console_handler = logging.StreamHandler()
console_handler.setLevel(logging.INFO)

# Create a formatter and add it to the handlers
formatter = logging.Formatter('%(asctime)s - %(name)s - %(levelname)s - %(message)s')
file_handler.setFormatter(formatter)
console_handler.setFormatter(formatter)

# Add the handlers to the logger
logger.addHandler(file_handler)
logger.addHandler(console_handler)

Base = declarative()

class Student(Base):
    __tablename__ = 'students'
    id = Column(Integer, primary_key=True)
    name = Column(String(length=50))  # specify length=50 for the name column
    email = Column(String(length=100))  # specify length=100 for the email column

class Course(Base):
    __tablename__ = 'courses'
    id = Column(Integer, primary_key=True)
    name = Column(String(length=50))  # specify length=50 for the name column
    description = Column(String(length=200))  # specify length=200 for the description column

class Enrollment(Base):
    __tablename__ = 'enrollments'
    id = Column(Integer, primary_key=True)
    student_id = Column(Integer, ForeignKey('students.id'))
    course_id = Column(Integer, ForeignKey('courses.id'))
    student = relationship("Student", backref="enrollments")
    course = relationship("Course", backref="enrollments")

engine = create_engine('mysql://root:Wwww1122@localhost/students')
logger.info('Creating database tables...')
Base.metadata.create_all(engine)
logger.info('Database tables created.')

Session = sessionmaker(bind=engine)
session = Session()

# Adding 5 courses
logger.info('Adding 5 courses...')
courses = [
    Course(name='Python Basics', description='Introduction to Python'),
    Course(name='Data Structures', description='Learn data structures in Python'),
    Course(name='Web Development', description='Build web applications with Python'),
    Course(name='Machine Learning', description='Introduction to machine learning with Python'),
    Course(name='Database Systems', description='Learn database systems with Python')
]
session.add_all(courses)
session.commit()
logger.info('5 courses added.')

# Adding 20 students and their distribution across courses
logger.info('Adding 20 students...')
students = []
for i in range(20):
    student = Student(name=f'Student {i+1}', email=f'student{i+1}@example.com')
    students.append(student)
    session.add(student)

session.commit()
logger.info('20 students added.')

courses_for_students = []
logger.info('Distributing students across courses...')
for student in students:
    for course in courses:
        if random.randint(0, 1):  # 50% chance of enrolling in a course
            enrollment = Enrollment(student=student, course=course)
            courses_for_students.append(enrollment)

session.add_all(courses_for_students)
session.commit()
logger.info('Students distributed across courses.')

# Functions for executing database queries
def get_students_on_course(course_name):
    logger.info(f'Getting students on course {course_name}...')
    return session.query(Student).join(Enrollment).join(Course).filter(Course.name == course_name).all()

def get_courses_for_student(student_name):
    logger.info(f'Getting courses for student {student_name}...')
    return session.query(Course).join(Enrollment).join(Student).filter(Student.name == student_name).all()

# Updating and deleting data
def update_student_email(student_name, new_email):
    logger.info(f'Updating email for student {student_name}...')
    student = session.query(Student).filter_by(name=student_name).first()
    if student:
        student.email = new_email
        session.commit()
        logger.info(f'Email updated for student {student_name}.')

def delete_student(student_name):
    logger.info(f'Deleting student {student_name}...')
    student = session.query(Student).filter_by(name=student_name).first()
    if student:
        session.delete(student)
        session.commit()
        logger.info(f'Student {student_name} deleted.')