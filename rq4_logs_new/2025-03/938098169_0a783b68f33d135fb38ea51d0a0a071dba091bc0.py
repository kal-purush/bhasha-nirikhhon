import pytest

from src.errors.types import InvalidDataError, NotFoundError
from tests.base_test import BaseTest


class TestStudendService(BaseTest):
    def test_enroll_student(
        self,
        student_service,
        create_student,
        create_canvas_user,
        create_canvas_course,
        enrollment_repo,
        cleanup_all,
    ):
        student = create_student()
        canvas_user = create_canvas_user()
        course = create_canvas_course(canvas_user=canvas_user)

        student_service.enroll_student(
            web_id=student.web_id, course_web_id=course.web_id
        )

        with enrollment_repo.session():
            enrollments = enrollment_repo.list_all()
            assert len(enrollments) == 1
            assert enrollments[0].course == course
            assert enrollments[0].student == student

    def test_enroll_student_should_raise_when_student_not_found(
        self,
        student_service,
        create_canvas_user,
        create_canvas_course,
        cleanup_all,
    ):
        canvas_user = create_canvas_user()
        course = create_canvas_course(canvas_user=canvas_user)
        with pytest.raises(NotFoundError) as exc:
            student_service.enroll_student(
                web_id="unknown-web-id", course_web_id=course.web_id
            )
        assert exc.value.message == "_error_msg_student_not_found:unknown-web-id"

    def test_enroll_student_should_raise_when_course_not_found(
        self,
        student_service,
        create_student,
        cleanup_all,
    ):
        student = create_student()
        with pytest.raises(NotFoundError) as exc:
            student_service.enroll_student(
                web_id=student.web_id, course_web_id="unknown-web-id"
            )
        assert exc.value.message == "_error_msg_course_not_found:unknown-web-id"

    def test_enroll_student_should_raise_when_enrollment_already_exists(
        self,
        student_service,
        create_student,
        create_canvas_user,
        create_enrollment,
        create_canvas_course,
        cleanup_all,
    ):
        student = create_student()
        canvas_user = create_canvas_user()
        course = create_canvas_course(canvas_user=canvas_user)
        create_enrollment(course=course, student=student)
        with pytest.raises(InvalidDataError) as exc:
            student_service.enroll_student(
                web_id=student.web_id, course_web_id=course.web_id
            )
        assert exc.value.message == "_error_msg_enrollment_already_exists"