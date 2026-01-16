from injector import Inject

from src.common.application.query_handler import QueryHandler
from src.modules.questionnaires.application.interactors.get_test_Session_by_id.get_test_Session_by_id_query import \
    GetTestSessionByIdQuery
from src.modules.questionnaires.application.interactors.get_test_Session_by_id.get_test_Session_by_id_query_service import \
    TestSessionByIdQueryService


class GetTestSessionByIdQueryHandler(QueryHandler[GetTestSessionByIdQuery, str]):
    def __init__(
            self,
            test_session_by_id_query_service: Inject[TestSessionByIdQueryService]
    ):
        self.__test_session_by_id_query_service = test_session_by_id_query_service

    async def handle(self, query: GetTestSessionByIdQuery) -> str:
        return await self.__test_session_by_id_query_service.execute_async(query)