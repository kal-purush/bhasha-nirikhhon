from datetime import datetime

from pydantic import BaseModel, ConfigDict, Field
from pydantic.alias_generators import to_camel

from src.common.domain.value_objects.cedula import Cedula
from src.common.domain.value_objects.email import Email
from src.common.domain.value_objects.sex import Sex
from src.modules.results_analysis.api.test_results.get_test_resuts_dto import TestResultsDto


class TestReportsDto(BaseModel):
    model_config = ConfigDict(alias_generator=to_camel)

    id: int
    child_id: int
    psychologist_cedula: str
    test_session_id: int
    child_age: int
    scholar_grade: int
    child_sex: str
    date_time_of_answer: datetime
    test_results: TestResultsDto
    time_taken: datetime