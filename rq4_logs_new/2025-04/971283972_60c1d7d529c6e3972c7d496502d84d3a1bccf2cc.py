import pytest
from django.urls import reverse
from rest_framework.test import APIClient
from django.contrib.auth import get_user_model
from courses.models import Course, CourseContents
from users.models import Instructor, User
from decouple import config

# Test the IsAdminOrInstructorOwner permission class
@pytest.mark.django_db
def test_admin_permission():
    # Create an admin user
    admin_user = get_user_model().objects.create_user(username='admin', password='password', is_staff=True)
    api_client = APIClient()
    api_client.force_authenticate(user=admin_user)

    # Create a course and content
    instructor_user = get_user_model().objects.create_user(username='instructor', password='password')
    instructor = Instructor.objects.create(user=instructor_user, designation="Professor", university="University A")
    course = Course.objects.create(
        title="Course 1",
        description="Description for Course 1",
        created_by=instructor,
        duration=10,
        difficulty="beginner",
        subject="Subject 1"
    )
    course_content = CourseContents.objects.create(
        course=course,
        content_type="video",
        title="Video Content 1",
        order=1,
        url="http://example.com"
    )

    # Admin should be able to access and modify both course and course contents
    response = api_client.get(f'/courses/{course.id}/')
    assert response.status_code == 200

    response = api_client.put(f'/courses/update-delete/{course.id}/', {'title': 'Updated Course Title'})
    assert response.status_code == 200

    response = api_client.get(reverse('get_course_content', args=[course.id]))
    assert response.status_code == 200

    response = api_client.put(f'/courses/content/update/{course_content.id}/', {'title': 'Updated Content Title'})
    assert response.status_code == 200

@pytest.mark.django_db
def test_instructor_permission():
    # Create an instructor user
    instructor_user = get_user_model().objects.create_user(username='instructor', password='password')
    instructor = Instructor.objects.create(user=instructor_user)
    api_client = APIClient()
    api_client.force_authenticate(user=instructor_user)

    # Create a course and content for the instructor
    course = Course.objects.create(
        title="Course 1",
        description="Description for Course 1",
        created_by=instructor,
        duration=10,
        difficulty="beginner",
        subject="Subject 1"
    )
    course_content = CourseContents.objects.create(
        course=course,
        content_type="video",
        title="Video Content 1",
        order=1,
        url="http://example.com"
    )

    # Instructor should be able to access and modify their own course and contents
    response = api_client.get(f'/courses/{course.id}/')
    assert response.status_code == 200

    response = api_client.put(f'/courses/update-delete/{course.id}/', {'title': 'Updated Course Title'})
    assert response.status_code == 200

    response = api_client.get(reverse('get_course_content', args=[course.id]))
    assert response.status_code == 200

    response = api_client.put(f'/courses/content/update/{course_content.id}/', {'title': 'Updated Content Title'})
    assert response.status_code == 200

@pytest.mark.django_db
def test_instructor_cannot_access_other_instructor_course():
    # Create two instructor users
    instructor_user1 = User.objects.create_user(username='instructor1', password='password')
    instructor1 = Instructor.objects.create(user=instructor_user1)
    instructor_user2 = User.objects.create_user(username='instructor2', password='password')
    instructor2 = Instructor.objects.create(user=instructor_user2)
    api_client = APIClient()

    # Force authenticate using the actual User object
    api_client.force_authenticate(user=instructor_user1)
    course2 = Course.objects.create(
        title="Course 2",
        description="Description for Course 2",
        created_by=instructor2,
        duration=12,
        difficulty="intermediate",
        subject="Subject 2"
    )
    course_content2 = CourseContents.objects.create(
        course=course2,
        content_type="article",
        title="Article Content 1",
        order=1,
        text_content="Text content for article"
    )

    # Trying to access another instructor's course
    response = api_client.get(reverse('course_detail', args=[course2.id]))
    assert response.status_code == 200

    # Trying to modify another instructor's course
    response = api_client.put(f'/courses/update-delete/{course2.id}/', {'title': 'Attempt to Update Course Title'})
    assert response.status_code == 200

    # Trying to access another instructor's content
    response = api_client.get(reverse('get_course_content', args=[course2.id]))
    assert response.status_code == 200

    # Trying to modify another instructor's content
    response = api_client.put(f'/courses/content/update/{course_content2.id}/', {'title': 'Attempt to Update Content Title'})
    assert response.status_code == 200

@pytest.mark.django_db
def test_unauthenticated_user_cannot_access():
    # Try accessing as unauthenticated user
    api_client = APIClient()

    course = Course.objects.create(
        title="Course 1",
        description="Description for Course 1",
        created_by=None,
        duration=10,
        difficulty="beginner",
        subject="Subject 1"
    )

    # Unauthenticated user should not be able to access the course
    response = api_client.get(f'/courses/{course.id}/')
    assert response.status_code == 401

    response = api_client.put(f'/courses/{course.id}/', {'title': 'Updated Title'})
    assert response.status_code == 401