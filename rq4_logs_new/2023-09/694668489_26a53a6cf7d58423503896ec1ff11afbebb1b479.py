from dataclasses import dataclass, field


@dataclass
class EpochSetStats:
    performance: dict[str, float] = field(default_factory=dict)
    epoch_count: int = 0
    observation_sequence_count: int = 0
    action_sequence_count: int = 0
    observation_sample_count: int = 0
    action_sample_count: int = 0


@dataclass
class EpochSetRecord:
    total: EpochSetStats = field(default_factory=EpochSetStats)
    best: EpochSetStats = field(default_factory=EpochSetStats)
    stop_reason: str | None = None

    def to_json(self) -> dict:
        return {"total": self.total.__dict__, "best": self.best.__dict__, "stop_reason": self.stop_reason}