from __future__ import annotations

from typing import NewType, TypedDict


class Data(TypedDict):
    spots: dict[SpotID, Spot]


SpotID = NewType("SpotID", int)


class Spot(TypedDict):
    id: SpotID
    name: str
    address: str
    uri: str