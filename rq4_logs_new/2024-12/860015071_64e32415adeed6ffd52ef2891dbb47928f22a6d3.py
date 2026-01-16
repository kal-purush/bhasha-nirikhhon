# 1. Створення моделі даних:
# Створіть просту модель даних для системи управління студентами.
# Модель може містити таблиці для студентів, курсів та їх відношень.
# Кожен студент може бути зареєстрований на декілька курсів.
# Наприклад, створити 5 курсів, та розподілити рандомно 20 студентів.
#
# 2. Виконання базових операцій:
# Напишіть програму, яка додає нового студента до бази даних та додає його до певного курсу.
# Переконайтеся, що ці зміни коректно відображаються у базі даних.
#
# 3. Запити до бази даних:
# Напишіть запити до бази даних,
# які повертають інформацію про студентів, зареєстрованих на певний курс,
# або курси, на які зареєстрований певний студент.
#
# 4. Оновлення та видалення даних:
# Реалізуйте можливість оновлення даних про студентів або курси,
# а також видалення студентів з бази даних.
# Можна використовувати будь яку ORM на Ваш вібир

from sqlalchemy import create_engine, Column, Integer, String, ForeignKey
from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy.orm import sessionmaker, relationship
import random

Base = declarative_base()

class Course(Base):
    __tablename__ = 'courses'
    id = Column(Integer, primary_key=True)
    name = Column(String, nullable=False)
    students = relationship("Student", back_populates="course")


class Student(Base):
    __tablename__ = 'students'

    id = Column(Integer, primary_key=True)
    name = Column(String, nullable=False)
    age = Column(Integer)

    course_id = Column(Integer, ForeignKey("courses.id"))
    course = relationship("Course", back_populates="students")

engine = create_engine("sqlite:///example.db", echo=True)
Base.metadata.create_all(engine)
Session = sessionmaker(bind=engine)
session = Session()



all_courses = [
    Course(name="History"),
    Course(name="Chemistry"),
    Course(name="Programming"),
    Course(name="Mathematics")
]
all_students = [
    Student(name='John', age=30, course=random.choice(all_courses)),
    Student(name='Vitalii', age=35, course=random.choice(all_courses)),
    Student(name='Andrew', age=33, course=random.choice(all_courses))
]


def print_all_students():
    print("print all students:")
    for student in session.query(Student).all():
        print(f"Student: {student.name}, Age: {student.age}, Course: {student.course.name}")


def add_new_student(name, age, course):
    course_db = session.query(Course).filter_by(name=course.name).first()
    student = Student(name=name, age=age, course=course)
    print(f"Adding new Student: {student.name}, Age: {student.age}, Course: {student.course.name}")
    if course_db:
        session.add(student)
        session.commit()


def delete_student(name):
    student = session.query(Student).filter_by(name=name).first()
    if student:
        session.delete(student)
        session.commit()
        print(f"Student {name} deleted.")


def get_student_by_name(name):
    return session.query(Student).filter_by(name=name).first()


def update_student(name, age, course):
    student = session.query(Student).filter_by(name=name).first()
    print("updating student")
    if student:
        print("Student = ", student)
        student.age = age
        student.course = course
        session.commit()


try:
    session.begin()
    session.add_all(all_courses)
    session.commit()
    session.add_all(all_students)
    session.commit()
    add_new_student("William", 44, course=random.choice(all_courses))
    update_student("John", 31, course=random.choice(all_courses))
    delete_student("Andrew")
    print_all_students()
except:
    session.rollback()
finally:
    session.close()