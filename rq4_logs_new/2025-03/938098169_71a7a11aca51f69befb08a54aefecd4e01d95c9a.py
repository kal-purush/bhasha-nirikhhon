from unittest.mock import patch

import pytest

from src.dto import auth_dto, canvas_course_dto
from src.errors.types import NotFoundError
from tests.base_test import BaseTest
from tests.fixtures import sample_data


class TestCanvasCourseService(BaseTest):
    def test_list_courses(
        self,
        create_canvas_course,
        create_canvas_user,
        canvas_course_service,
        cleanup_all,
    ):
        canvas_user = create_canvas_user()
        for i in range(5):
            create_canvas_course(
                web_id=f"web-id-{i}",
                long_name=f"test-long_name{i}",
                canvas_course_id=i,
                canvas_user=canvas_user,
            )
        courses = canvas_course_service.list_courses()
        assert courses.page == 1
        assert courses.page_size == 10
        assert courses.total == 5
        assert len(courses.items) == 5

    def test_list_courses_filtered_by_user(
        self,
        create_canvas_course,
        create_canvas_user,
        canvas_course_service,
        cleanup_all,
    ):
        canvas_user = create_canvas_user()
        for i in range(2):
            create_canvas_course(
                web_id=f"web-id-{2 + i}",
                long_name=f"test{i}",
                canvas_user=canvas_user,
                canvas_course_id=i,
            )

        another_user = create_canvas_user(
            username="another@.com",
            web_id="web-id-2",
            canvas_id="canvas_id_228",
        )
        for i in range(5):
            create_canvas_course(
                web_id=f"another_web-id-{2 + i}",
                long_name=f"another_test{i}",
                canvas_user=another_user,
                canvas_course_id=i + 5,
            )
        filter_params = canvas_course_dto.FilterParams(canvas_user_id=canvas_user.id)
        courses = canvas_course_service.list_courses(filter_params=filter_params)
        assert courses.page == 1
        assert courses.page_size == 10
        assert courses.total == 2
        for course in courses.items:
            assert course.owner_username == canvas_user.username

    @pytest.mark.asyncio
    @patch("aiohttp.ClientSession.get")
    async def test_load_courses(
        self,
        mock_get,
        canvas_course_ok_response,
        canvas_course_service,
        create_canvas_user,
        canvas_course_repo,
        cleanup_all,
    ):
        mock_get.return_value = canvas_course_ok_response
        canvas_user = create_canvas_user()
        canvas_auth_data = auth_dto.CanvasAuthData(**sample_data.canvas_auth_data)

        courses = await canvas_course_service.load_courses(
            canvas_user_web_id=canvas_user.web_id, canvas_auth_data=canvas_auth_data
        )

        with canvas_course_repo.session():
            courses = canvas_course_repo.list_all()
            assert len(courses) == 2
            for course in courses:
                assert course.canvas_user == canvas_user

        mock_get.assert_called_once_with(
            "https://canvas.narxoz.kz/api/v1/dashboard/dashboard_cards",
            cookies={
                "_csrf_token": "6D2%2FyVAKZOxBRl6qZW",
                "_legacy_normandy_session": "pG-loNiNeyypme8vr5TO",
                "_normandy_session": "pG-loNiNeyypme8vr5TO",
                "log_session_id": "9f8187d804aaf1d15913",
            },
        )

    @pytest.mark.asyncio
    @patch("aiohttp.ClientSession.get")
    async def test_load_courses_should_raise_on_invalid_user(
        self,
        mock_get,
        canvas_course_ok_response,
        canvas_course_service,
        create_canvas_user,
        cleanup_all,
    ):
        mock_get.return_value = canvas_course_ok_response
        canvas_auth_data = auth_dto.CanvasAuthData(**sample_data.canvas_auth_data)

        with pytest.raises(NotFoundError) as exc:
            await canvas_course_service.load_courses(
                canvas_user_web_id="unknown-user-id", canvas_auth_data=canvas_auth_data
            )
        assert exc.value.message == "_error_msg_user_not_found:unknown-user-id"

    @pytest.mark.asyncio
    @patch("aiohttp.ClientSession.get")
    async def test_load_courses_should_not_create_duplicates(
        self,
        mock_get,
        canvas_course_ok_response,
        canvas_course_service,
        create_canvas_user,
        canvas_course_repo,
        cleanup_all,
    ):
        mock_get.return_value = canvas_course_ok_response
        canvas_user = create_canvas_user()
        canvas_auth_data = auth_dto.CanvasAuthData(**sample_data.canvas_auth_data)

        courses = await canvas_course_service.load_courses(
            canvas_user_web_id=canvas_user.web_id, canvas_auth_data=canvas_auth_data
        )

        with canvas_course_repo.session():
            courses = canvas_course_repo.list_all()
            assert len(courses) == len(sample_data.canvas_courses_data)

        courses = await canvas_course_service.load_courses(
            canvas_user_web_id=canvas_user.web_id, canvas_auth_data=canvas_auth_data
        )

        with canvas_course_repo.session():
            courses = canvas_course_repo.list_all()
            assert len(courses) == len(sample_data.canvas_courses_data)