from src.enums.attendance_status import AttendanceStatus
from src.enums.attendance_value import AttendanceValue
from tests.base_test import BaseTest
from tests.web.web_application_test import WebApplicationTest


class TestAttendanceRouter(BaseTest, WebApplicationTest):

    def test_list_attendance_by_assignment_id(
        self,
        client,
        create_canvas_user,
        create_canvas_course,
        create_assignment,
        create_assignment_group,
        create_attendance,
        create_student,
        cleanup_all,
    ):
        canvas_user = create_canvas_user(username="user")
        course = create_canvas_course(canvas_user=canvas_user)
        assignment_group = create_assignment_group(course=course, name="Test Group")
        assignment = create_assignment(
            assignment_group=assignment_group, name="sample assignment"
        )
        for _ in range(5):
            student = create_student()
            create_attendance(student=student, assignment=assignment)
        response = client.get(
            f"/api/attendances/v1/?assignment_web_id={assignment.web_id}&page=1&page_size=100"
        )
        assert response.status_code == 200
        data = response.json()
        assert data["page"] == 1
        assert data["page_size"] == 100
        assert data["total"] == 5
        assert len(data["items"]) == 5
        assert data["items"] == [
            {
                "web_id": "web-id-7",
                "assignment_id": 1,
                "status": "INITIATED",
                "value": "complete",
                "student": {
                    "id": 1,
                    "web_id": "web-id-6",
                    "name": "Test Testname",
                    "email": "test@gmail.com",
                },
            },
            {
                "web_id": "web-id-9",
                "assignment_id": 1,
                "status": "INITIATED",
                "value": "complete",
                "student": {
                    "id": 2,
                    "web_id": "web-id-8",
                    "name": "Test Testname",
                    "email": "test@gmail.com",
                },
            },
            {
                "web_id": "web-id-11",
                "assignment_id": 1,
                "status": "INITIATED",
                "value": "complete",
                "student": {
                    "id": 3,
                    "web_id": "web-id-10",
                    "name": "Test Testname",
                    "email": "test@gmail.com",
                },
            },
            {
                "web_id": "web-id-13",
                "assignment_id": 1,
                "status": "INITIATED",
                "value": "complete",
                "student": {
                    "id": 4,
                    "web_id": "web-id-12",
                    "name": "Test Testname",
                    "email": "test@gmail.com",
                },
            },
            {
                "web_id": "web-id-15",
                "assignment_id": 1,
                "status": "INITIATED",
                "value": "complete",
                "student": {
                    "id": 5,
                    "web_id": "web-id-14",
                    "name": "Test Testname",
                    "email": "test@gmail.com",
                },
            },
        ]

    def test_list_attendances_by_assignment_should_raise_on_invalid_assignment_web_id(
        self,
        client,
        cleanup_all,
    ):
        response = client.get("/api/attendances/v1/?assignment_web_id=unknown-web-id")
        assert response.status_code == 404
        data = response.json()
        assert data["message"] == "_error_msg_assignment_not_found:unknown-web-id"

    def test_mark_attendance(
        self,
        client,
        create_canvas_user,
        create_canvas_course,
        create_assignment,
        create_assignment_group,
        create_attendance,
        create_student,
        cleanup_all,
    ):
        canvas_user = create_canvas_user(username="user")
        course = create_canvas_course(canvas_user=canvas_user)
        assignment_group = create_assignment_group(course=course, name="Test Group")
        assignment = create_assignment(
            assignment_group=assignment_group, name="sample assignment"
        )
        student = create_student()
        attendance = create_attendance(
            student=student,
            assignment=assignment,
            status=AttendanceStatus.COMPLETED,
            value=AttendanceValue.EXCUSE,
        )
        response = client.put(
            f"/api/attendances/v1/{attendance.web_id}",
            json={"value": AttendanceValue.COMPLETE.value},
        )
        assert response.status_code == 201
        data = response.json()
        assert data == {
            "web_id": "web-id-7",
            "assignment_id": 1,
            "status": "INITIATED",
            "value": "complete",
            "student": {
                "id": 1,
                "web_id": "web-id-6",
                "name": "Test Testname",
                "email": "test@gmail.com",
            },
        }

    def test_mark_attendance_should_raise_on_invalid_web_id(
        self,
        client,
        cleanup_all,
    ):
        response = client.put(
            "/api/attendances/v1/unknown-web-id",
            json={"value": AttendanceValue.COMPLETE.value},
        )
        assert response.status_code == 404
        data = response.json()
        assert data["message"] == "_error_msg_attendance_not_found: unknown-web-id"