from src.dto import auth_dto, canvas_assignment_dto
from src.errors.types import NotFoundError
from src.proxies.canvas_proxy_provider import CanvasProxyProvider
from src.repositories.canvas_course_repo import CanvasCourseRepo


class CanvasAssignmentService:
    def __init__(
        self,
        canvas_course_repo: CanvasCourseRepo,
        canvas_proxy_provider_cls=CanvasProxyProvider,
    ):
        self._canvas_proxy_provider = canvas_proxy_provider_cls()
        self._canvas_course_repo = canvas_course_repo

    def list_assignments(self, course_id: int, auth_dto: auth_dto.CanvasAuthData):
        pass

    async def list_attendance_assignment_group(
        self, web_id: str, dto: auth_dto.CanvasAuthData
    ) -> list[canvas_assignment_dto.AssignmentGroup]:
        with self._canvas_course_repo.session():
            course = self._canvas_course_repo.get_by_web_id(web_id=web_id)
            if not course:
                raise NotFoundError(message=f"_err_message_course_not_found:{web_id}")
        group = await self._canvas_proxy_provider.list_attendance_assignment_group(
            cookies=dto, course_id=course.canvas_course_id
        )
        return group

    def create_assignment(self):
        pass