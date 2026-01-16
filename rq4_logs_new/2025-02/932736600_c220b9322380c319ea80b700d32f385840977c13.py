from pydantic import BaseModel, Field
from datetime import datetime
from typing import List, Dict

class Location(BaseModel):
    latitude: float
    longitude: float
    address: str

class RiderDetail(BaseModel):
    pickupLocation: Location
    dropoffLocation: Location
    rideStatus: str

class Ride(BaseModel):
    rideId: str
    driverId: str
    startLocation: Location
    endLocation: Location
    startTime: datetime
    endTime: datetime
    daysOfWeek: List[str] | None = None
    availableSeats: int
    riders: Dict[str, RiderDetail] | None = None

    class Config:
        json_encoders = {
            datetime: lambda v: v.isoformat(),
        }