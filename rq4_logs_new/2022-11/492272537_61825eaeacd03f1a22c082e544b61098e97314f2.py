from enum import Enum, unique


@unique
class DiscourseType(Enum):
    LEAD = "Lead"
    POSITION = "Position"
    CLAIM = "Claim"
    EVIDENCE = "Evidence"
    COUNTERCLAIM = "Counterclaim"
    CONCLUDING_STATEMENT = "Concluding Statement"
    REBUTTAL = "Rebuttal"