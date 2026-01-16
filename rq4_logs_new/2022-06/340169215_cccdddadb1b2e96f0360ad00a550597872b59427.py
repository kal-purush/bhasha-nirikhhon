from enum import Enum

class Strategy(Enum):
    STRATEGY_0 = "Full Treatment with Standard Precision Medicine (Strategy 0 PNAS2012)"
    STRATEGY_2_2_TRIAL = "First 2 Treament Selections with DPM (Strategy 2.2 PNAS2012)"
    STRATEGY_2_2 = "Full Treatment with DPM (Strategy 2.2 PNAS2012)"

class TrialOutcome(Enum):
    BOTH_SAME = "Recommendations matched for BOTH evaluation windows"
    FIRST_SAME = "Recommendations matched for FIRST evaluation only"
    SECOND_SAME = "Recommendation matched for SECOND evaluation only"
    NONE_SAME = "Recommendation matched for NO evaluation windows"