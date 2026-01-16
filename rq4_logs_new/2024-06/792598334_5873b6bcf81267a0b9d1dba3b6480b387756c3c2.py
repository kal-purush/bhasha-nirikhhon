from fastapi import APIRouter, HTTPException
from fastapi_injector import Injected
from mediatr import Mediator, mediator

from src.modules.questionnaires.api.questionnaires_children.validate_token_dto import ValidateTokenDto
from src.modules.questionnaires.application.interactors.validate_questionnaire_token.validate_questionnaire_token_query import \
    ValidateQuestionnaireTokenQuery

router = APIRouter(prefix="/questionnaires", tags=["Questionnaires"])


@router.post("/validate-token", response_model=None)
async def validate_token(validate_token_dto: ValidateTokenDto, mediator: Mediator = Injected(Mediator)):
    query = ValidateQuestionnaireTokenQuery(
        token=validate_token_dto.token
    )
    if not await mediator.send_async(query):
        raise HTTPException(status_code=401, detail=None)