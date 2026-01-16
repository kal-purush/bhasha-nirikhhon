from src.common.application.integration_event_handler import IntegrationEventHandler
from src.modules.patients.domain.psychologist.psychologist import Psychologist
from src.modules.patients.domain.psychologist.psychologist_repository_async import PsychologistRepositoryAsync
from src.modules.users_management.integration_events.psychologist_updated_event import PsychologistUpdatedEvent


class PsychologistUpdatedEventHandler(IntegrationEventHandler[PsychologistUpdatedEvent, None]):
    def __init__(self, psychologist_repository_async: PsychologistRepositoryAsync):
        self.__psychologist_repository_async = psychologist_repository_async

    async def handle(self, event: PsychologistUpdatedEvent) -> None:
        psychologist = await self.__psychologist_repository_async.get_by_cedula(event.cedula)

        # Update psychologist details
        psychologist.name = event.name
        psychologist.sex = event.sex
        psychologist.email = event.email

        await self.__psychologist_repository_async.update_psychologist(psychologist)