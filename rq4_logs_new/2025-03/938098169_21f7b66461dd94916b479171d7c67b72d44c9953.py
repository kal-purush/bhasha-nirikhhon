from unittest.mock import AsyncMock, patch

import pytest

from src.dto import auth_dto, canvas_assignment_dto
from src.enums.attendance_status import AttendanceStatus
from src.enums.attendance_value import AttendanceValue
from src.errors.types import NotFoundError
from tests.base_test import BaseTest
from tests.fixtures import sample_data


class TestCanvasAssignmentService(BaseTest):
    @pytest.fixture
    def canvas_assignment_secure_params_response(self):
        mock_response = AsyncMock()
        mock_response.__aenter__.return_value = mock_response
        mock_response.__aexit__.return_value = None
        mock_response.text.return_value = (
            sample_data.canvas_assignment_secure_params_response
        )
        return mock_response

    @pytest.fixture
    def canvas_assignment_response(self):
        mock_response = AsyncMock()
        mock_response.__aenter__.return_value = mock_response
        mock_response.__aexit__.return_value = None
        mock_response.json.return_value = sample_data.canvas_assignment_response
        return mock_response

    @pytest.mark.asyncio
    @patch("aiohttp.ClientSession.get")
    @patch("aiohttp.ClientSession.post")
    async def test_create_assignment(
        self,
        mock_post,
        mock_get,
        canvas_assignment_service,
        create_canvas_user,
        create_canvas_course,
        assignment_repo,
        canvas_assignment_secure_params_response,
        canvas_assignment_response,
        cleanup_all,
    ):
        mock_get.return_value = canvas_assignment_secure_params_response
        mock_post.return_value = canvas_assignment_response
        canvas_user = create_canvas_user(username="user")
        course = create_canvas_course(canvas_user=canvas_user)
        dto = canvas_assignment_dto.Create(assignment_group_id=1)
        canvas_auth_data = auth_dto.CanvasAuthData(**sample_data.canvas_auth_data)
        assignment = await canvas_assignment_service.create_assignment(
            web_id=course.web_id, dto=dto, canvas_auth_data=canvas_auth_data
        )
        assert assignment.name == "test from python finally"
        assert assignment.course_id == course.id
        assert assignment.canvas_assignment_id == 73376
        with assignment_repo.session():
            assignments = assignment_repo.list_all()
            assert len(assignments) == 1
            assert assignments[0].name == "test from python finally"
            assert assignments[0].course_id == course.id
            assert assignments[0].canvas_assignment_id == 73376

    @pytest.mark.asyncio
    @patch("aiohttp.ClientSession.get")
    @patch("aiohttp.ClientSession.post")
    async def test_create_assignment_should_create_attendance_for_each_enrollment(
        self,
        mock_post,
        mock_get,
        canvas_assignment_service,
        create_canvas_user,
        create_canvas_course,
        create_student,
        create_enrollment,
        attendance_repo,
        canvas_assignment_secure_params_response,
        canvas_assignment_response,
        cleanup_all,
    ):
        mock_get.return_value = canvas_assignment_secure_params_response
        mock_post.return_value = canvas_assignment_response
        canvas_user = create_canvas_user(username="user")
        course = create_canvas_course(canvas_user=canvas_user)
        for _ in range(5):
            student = create_student()
            create_enrollment(course=course, student=student)

        dto = canvas_assignment_dto.Create(assignment_group_id=1)
        canvas_auth_data = auth_dto.CanvasAuthData(**sample_data.canvas_auth_data)
        assignment = await canvas_assignment_service.create_assignment(
            web_id=course.web_id, dto=dto, canvas_auth_data=canvas_auth_data
        )
        with attendance_repo.session():
            attendances = attendance_repo.list_all()
            assert len(attendances) == 5
            for att in attendances:
                assert att.assignment.web_id == assignment.web_id
                assert att.status == AttendanceStatus.INITIATED
                assert att.value == AttendanceValue.INCOMPLETE

    @pytest.mark.asyncio
    async def test_create_assignment_should_raise_on_invalid_course(
        self,
        canvas_assignment_service,
        cleanup_all,
    ):
        dto = canvas_assignment_dto.Create(assignment_group_id=1)
        canvas_auth_data = auth_dto.CanvasAuthData(**sample_data.canvas_auth_data)
        with pytest.raises(NotFoundError) as exc:
            await canvas_assignment_service.create_assignment(
                web_id="unknown-web-id", dto=dto, canvas_auth_data=canvas_auth_data
            )
        assert exc.value.message == "_err_message_course_not_found:unknown-web-id"