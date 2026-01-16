from injector import Inject

from src.common.application.query_handler import QueryHandler
from src.modules.patients.application.interactors.get_children.get_children_query import GetChildrenQuery
from src.modules.patients.application.interactors.get_children.get_children_query_service import ChildrenQueryService


class GetChildrenQueryHandler(QueryHandler[GetChildrenQuery, str]):
    def __init__(
            self,
            children_query_service: Inject[ChildrenQueryService]
    ):
        self.__children_query_service = children_query_service

    async def handle(self, query: GetChildrenQuery) -> str:
        return await self.__children_query_service.execute_async(query)