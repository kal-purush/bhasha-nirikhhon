from enum import Enum

class JobPositionType(Enum):
    CONTRACT = "CONTRACT"
    FREELANCE = "FREELANCE"
    FULL_TIME = "FULL TIME"
    INTERNSHIP = "INTERNSHIP"
    PART_TIME = "PART TIME"
    TEMPORARY = "TEMPORARY"
    VOLUNTEER = "VOLUNTEER"

class ApprovalStatus(Enum):
    APPROVED = "APPROVED"
    PENDING = "PENDING"
    REJECTED = "REJECTED"