from pydantic import BaseModel
from typing import List

import instructor
from openai import OpenAI

from routes.course_chat_misconceptions.schemas import CourseChatMisconception

instructor_client = instructor.from_openai(OpenAI())


class MisconceptionsExtractorResponse(BaseModel):
    misconceptions: List[str]


def extract_misconceptions(message: str) -> List[str]:
    messages = [
        {
            "role": "system",
            "content": "You are an expert at extracting student misconceptions in a message by a student chatting with an AI tutor for a course. A misconception should be phrased as what the student struggled with. Extract unique misconceptions and keep them concise. Do not number them.",
        },
        {"role": "user", "content": "Message: {}".format(message)},
    ]

    result = instructor_client.chat.completions.create(
        model="gpt-3.5-turbo",
        response_model=MisconceptionsExtractorResponse,
        messages=messages,
    )

    return result.misconceptions


class NewMisconceptionsExtractorResponse(BaseModel):
    new_misconceptions: List[str]
    used_misconception_ids: List[int]


def extract_new_and_old_misconceptions(
    existing_misconceptions: List[CourseChatMisconception],
    new_misconceptions: List[str],
) -> NewMisconceptionsExtractorResponse:
    messages = [
        {
            "role": "system",
            "content": "You are an expert at extracting new misconceptions by comparing two lists.Compare new misconceptions with existing misconceptions by meaning and remove duplicate misconceptions in the new misconceptions list that are already there in the existing misconceptions list. You should also return the IDs of the misconceptions that are used in both lists.",
        },
        {
            "role": "user",
            "content": "Existing misconceptions: "
            + str([topic.name for topic in existing_misconceptions])
            + "\nNew misconceptions: "
            + str(new_misconceptions),
        },
    ]

    result = instructor_client.chat.completions.create(
        model="gpt-3.5-turbo",
        response_model=NewMisconceptionsExtractorResponse,
        messages=messages,
    )

    return result