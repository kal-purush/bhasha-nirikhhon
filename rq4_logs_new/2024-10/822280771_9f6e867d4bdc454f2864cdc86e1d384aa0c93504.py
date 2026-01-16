from django.shortcuts import get_object_or_404
from ninja import Query, Router
from ninja.errors import HttpError
from ninja.pagination import PageNumberPagination, paginate

from polls.api_schemas import (
    BasePollSchema,
    Error,
    ExistingPollSchema,
    QuestionSchema,
)
from polls.models import Poll, PollQuestion
from users.auth import AuthBearer


router = Router(tags=["polls"])


@router.post("/", response={201: ExistingPollSchema, 400: str}, auth=AuthBearer())
def create_poll(request, payload: BasePollSchema):
    try:  # noqa: WPS229
        poll = Poll.objects.create(author_id=request.auth.pk, **payload.dict())
        poll.save()
    except Exception as ex:
        return 400, f"An unexpected error occurred: {str(ex)}"  # noqa: WPS237
    return 201, ExistingPollSchema.model_validate(poll)


@router.put("/{poll_id}", response={200: ExistingPollSchema, 400: str}, auth=AuthBearer())
def update_poll(request, poll_id: int, payload: BasePollSchema):
    poll = get_object_or_404(Poll, pk=poll_id)
    try:  # noqa: WPS229
        # Update the instance
        for attr, value in payload.dict().items():
            setattr(poll, attr, value)
        poll.save()
    except Exception as ex:
        return 400, f"An unexpected error occurred: {str(ex)}"  # noqa: WPS237
    return 200, ExistingPollSchema.model_validate(poll)


@router.get("/{poll_id}", response={200: ExistingPollSchema, 404: str}, auth=AuthBearer())
def get_poll(request, poll_id: int):
    poll = get_object_or_404(Poll, id=poll_id)
    return ExistingPollSchema.model_validate(poll)


def parse_statuses(value: str) -> list[int]:
    """Parse a comma-separated string of integers into a list of integers."""
    return [int(status) for status in value.split(",") if status.isdigit()]


def validate_statuses(values: list[int]) -> None:
    """Check statuses for presence in Poll model. Raise HttpError if status is not present."""
    for status in values:
        if status not in Poll.poll_statuses():
            raise HttpError(
                422,
                f"Incorrect status {status}. " +
                f"Choose from {', '.join(map(str, Poll.poll_statuses()))}",  # noqa: WPS237
            )


@router.get("/", response={200: list[ExistingPollSchema], 404: Error}, auth=AuthBearer())  # noqa: WPS221
@paginate(PageNumberPagination, page_size=15)
def list_polls(request, statuses: str = Query("0")):  # noqa: WPS404, B008
    statuses_list = parse_statuses(statuses)

    if len(statuses_list) == 1 and statuses_list[0] == 0:
        return Poll.objects.all()

    validate_statuses(statuses_list)  # Validate statuses, raise HttpError if failed

    return Poll.objects.filter(status__in=statuses_list)


# Questions

@router.post("/{poll_id}/questions", response={201: str, 400: str}, auth=AuthBearer())
def create_questions(request, poll_id: int, questions: list[QuestionSchema]):
    """Добавление вопросов для опроса."""
    try:
        for question in questions:
            poll_question = PollQuestion.objects.create(poll_id=poll_id, **question.dict())
            poll_question.save()
    except Exception as ex:
        return 400, f"An unexpected error occurred: {str(ex)}"  # noqa: WPS237
    return 201, "Created"


@router.get(
    "/{poll_id}/questions", response={200: list[QuestionSchema], 400: str}, auth=AuthBearer(),  # noqa: WPS221
)
def list_questions(request, poll_id: int):
    """Список вопросов для опроса."""
    questions = PollQuestion.objects.filter(poll_id=poll_id)
    return [QuestionSchema.model_validate(question) for question in questions]