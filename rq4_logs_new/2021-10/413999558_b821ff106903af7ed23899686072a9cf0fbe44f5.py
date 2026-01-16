from user_research.types.algorithms import StatisticalTestFamily
from user_research.types.basics import AnalysisType


def test_StatisticalTestFamily():
    t = StatisticalTestFamily(
        is_discrete=False,
        analysis_type=AnalysisType.PRECISION_ESTIMATE,
        is_against_benchmark=False,
        is_task_time=False,
    )

    assert isinstance(t, StatisticalTestFamily)