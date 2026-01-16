from typing import List

from pydantic import BaseModel, ConfigDict, Field, field_validator
from pydantic.alias_generators import to_camel


class AnswerDto(BaseModel):
    model_config = ConfigDict(alias_generator=to_camel)

    question_id: int = Field(gt=0, lt=50)
    answer: bool
    time_taken: int = Field(gt=0)


class SetQuestionnaireAnswersDto(BaseModel):
    model_config = ConfigDict(alias_generator=to_camel)

    token: str
    answers: List[AnswerDto] = Field(min_items=49, max_items=49)

    @field_validator('answers')
    def validate_question_ids(cls, answers):
        question_ids = [answer.question_id for answer in answers]
        if set(question_ids) != set(range(1, 50)):
            raise ValueError("The question IDs must include all numbers from 1 to 49 without repetition.")
        return answers