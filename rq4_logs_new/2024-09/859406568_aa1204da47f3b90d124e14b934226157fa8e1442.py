from typing import List

from models.AnswerModel import Answer


def create_answers(question_id: int, incorrect_answers: List[str]) -> List[Answer]:
    return [
        Answer(question_id=question_id, incorrect_answer=answer)
        for answer in incorrect_answers
    ]

question_id = 1
incorrect_answers = ["Answer 1", "Answer 2", "Answer 3"]

answers = create_answers(question_id, incorrect_answers)

