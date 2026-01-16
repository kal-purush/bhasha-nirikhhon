from __future__ import annotations

from dataclasses import dataclass
from typing import TYPE_CHECKING

from beartype import beartype
from rates import GammaRate

if TYPE_CHECKING:
    from interfaces import CleanedInput


@dataclass
class Gamma:
    @beartype
    def calculate(self, readings: CleanedInput) -> GammaRate:
        BIT_LENGTH = len(readings[0])
        gamma = ""

        for bit_index in range(len(readings[0])):
            tally = {"0": 0, "1": 0}
            for reading in readings:
                assert BIT_LENGTH == len(reading)
                tally[reading[bit_index]] += 1
            gamma += max(tally, key=tally.get)
        return GammaRate(binary=gamma)
