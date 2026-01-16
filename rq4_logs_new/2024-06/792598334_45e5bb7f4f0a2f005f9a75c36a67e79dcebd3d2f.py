from injector import Inject

from src.common.application.command_handler import CommandHandler
from src.common.application.event_bus import EventBus
from src.modules.questionnaires.application.interactors.delete_test_by_id.delete_test_by_id_command import \
    DeleteTestByIdCommand
from src.modules.questionnaires.domain.test_session.test_session_repository_async import TestSessionRepositoryAsync
from src.modules.questionnaires.integration_events.test_session_deleted_event import TestSessionDeletedEvent


class DeleteTestByIdCommandHandler(CommandHandler[DeleteTestByIdCommand, None]):
    def __init__(
            self,
            event_bus: Inject[EventBus],
            test_session_repository: Inject[TestSessionRepositoryAsync]
    ):
        self.__event_bus = event_bus
        self.__test_session_repository = test_session_repository

    async def handle(self, command: DeleteTestByIdCommand) -> None:
        await self.__test_session_repository.delete_test_session_by_id(command.psychologist_cedula,
                                                                       command.test_session_id)

        self.__event_bus.publish(
            TestSessionDeletedEvent(
                test_session_id=command.test_session_id
            )
        )