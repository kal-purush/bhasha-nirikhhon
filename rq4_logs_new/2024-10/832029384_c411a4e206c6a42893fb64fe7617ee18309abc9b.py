import pytest
from sqlalchemy import create_engine, inspect, text
from sqlalchemy.orm import sessionmaker
from lesson_22.students import Base, Student, Course, Enrollment

@pytest.fixture
def session():
    # Create an engine for SQLite in memory
    engine = create_engine('sqlite:///:memory:', echo=False)

    # Create all tables
    Base.metadata.create_all(engine)

    # Create a session
    TestingSessionLocal = sessionmaker(bind=engine)
    session = TestingSessionLocal()

    # Add test data
    courses = [
        Course(name='Python Basics', description='Introduction to Python'),
        Course(name='Data Structures', description='Learn data structures in Python')
    ]
    session.add_all(courses)
    session.commit()

    students = [
        Student(name='John Doe', email='john@example.com'),
        Student(name='Jane Doe', email='jane@example.com')
    ]
    session.add_all(students)
    session.commit()

    enrollments = [
        Enrollment(student_id=students[0].id, course_id=courses[0].id),
        Enrollment(student_id=students[1].id, course_id=courses[1].id)
    ]
    session.add_all(enrollments)
    session.commit()

    yield session

    # Close the session after tests
    session.close()

# Test for checking database connection
def test_database_connection(session):
    # Perform a simple query to check connection
    try:
        session.execute(text('SELECT 1'))  # Use text()
    except Exception as e:
        pytest.fail(f"Cannot connect to the database: {e}")

# Test to check if tables are created in the database
def test_tables_exist(session):
    inspector = inspect(session.get_bind())
    tables = inspector.get_table_names()

    assert 'courses' in tables, "Table 'courses' not found"
    assert 'students' in tables, "Table 'students' not found"
    assert 'enrollments' in tables, "Table 'enrollments' not found"

    # Check for all columns in each table
    course_columns = [col['name'] for col in inspector.get_columns('courses')]
    student_columns = [col['name'] for col in inspector.get_columns('students')]
    enrollment_columns = [col['name'] for col in inspector.get_columns('enrollments')]

    assert 'name' in course_columns, "Column 'name' not found in table 'courses'"
    assert 'description' in course_columns, "Column 'description' not found in table 'courses'"

    assert 'name' in student_columns, "Column 'name' not found in table 'students'"
    assert 'email' in student_columns, "Column 'email' not found in table 'students'"

    assert 'student_id' in enrollment_columns, "Column 'student_id' not found in table 'enrollments'"
    assert 'course_id' in enrollment_columns, "Column 'course_id' not found in table 'enrollments'"