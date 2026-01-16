from fit_galgo.fit.models import (
    FileId,
    Lap,
    Record,
    Session,
    Set,
    Split,
    Workout,
    WorkoutStep,
    MonitoringHrData,
    StressLevel,
    RespirationRate,
    MonitoringInfo,
    Monitoring,
    HrvStatusSummary,
    HrvValue,
    SleepLevel,
    SleepAssessment
)


# All messages supported.
# Each message (whose key is in the FIT SDK Profile) has:
# - The name you can find in the FIT SDK Profile.
# - The number you can find in the FIT SDK Profile.
# - The class message to build the message from garmin_fit_sdk library.
MESSAGES = {
    "FILE_ID": {
        "name": "FILE_ID",
        "num": 0,
        "model_cls": FileId
    },

    # ACTIVITY DEFINITIONS
    # "SPORT": {
    #     "name": "SPORT",
    #     "num": 12,
    #     "model_cls": FitSportMesg
    # },
    # "TRAINING_FILE": {
    #     "name": "TRAINING_FILE",
    #     "num": 72,
    #     "model_cls": None
    # },
    "WORKOUT": {
        "name": "WORKOUT",
        "num": 26,
        "model_cls": Workout
    },
    "WORKOUT_STEP": {
        "name": "WORKOUT_STEP",
        "num": 27,
        "model_cls": WorkoutStep
    },
    "RECORD": {
        "name": "RECORD",
        "num": 20,
        "model_cls": Record
    },
    "LAP": {
        "name": "LAP",
        "num": 19,
        "model_cls": Lap
    },
    "SET": {
        "name": "SET",
        "num": 225,
        "model_cls": Set
    },
    "SPLIT": {
        "name": "SPLIT",
        "num": 312,
        "model_cls": Split
    },
    # "TIME_IN_ZONE": {
    #     "name": "TIME_IN_ZONE",
    #     "num": 216,
    #     "model_cls": None
    # },
    "SESSION": {
        "name": "SESSION",
        "num": 18,
        "model_cls": Session
    },
    # "ACTIVITY": {
    #     "name": "ACTIVITY",
    #     "num": 34,
    #     "model_cls": None
    # }

    # MONITOR DEFINITIONS
    "MONITORING_INFO": {
        "name": "MONITORING_INFO",
        "num": 103,
        "model_cls": MonitoringInfo
    },
    "MONITORING": {
        "name": "MONITORING",
        "num": 55,
        "model_cls": Monitoring
    },
    "MONITORING_HR_DATA": {
        "name": "MONITORING_HR_DATA",
        "num": 211,
        "model_cls": MonitoringHrData
    },
    "STRESS_LEVEL": {
        "name": "STRESS_LEVEL",
        "num": 227,
        "model_cls": StressLevel
    },
    "RESPIRATION_RATE": {
        "name": "RESPIRATION_RATE",
        "num": 297,
        "model_cls": RespirationRate
    },

    # SLEEP AND HRV DEFINITIONS
    "SLEEP_LEVEL": {
        "name": "SLEEP_LEVEL",
        "num": 275,
        "model_cls": SleepLevel
    },
    "SLEEP_ASSESSMENT": {
        "name": "SLEEP_ASSESSMENT",
        "num": 346,
        "model_cls": SleepAssessment
    },
    "HRV_STATUS_SUMMARY": {
        "name": "HRV_STATUS_SUMMARY",
        "num": 370,
        "model_cls": HrvStatusSummary
    },
    "HRV_VALUE": {
        "name": "HRV_VALUE",
        "num": 371,
        "model_cls": HrvValue
    }
}