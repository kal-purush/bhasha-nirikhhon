from fastapi import APIRouter, Depends
from sqlalchemy.orm import Session
import schemas.trip_schema as trip_schema
import repositories.trip_repo as trip_repo
from database import get_db

router = APIRouter()

# Post a new trip
@router.post("/trips/", response_model=trip_schema.TripResponse)
def create_trip(trip: trip_schema.TripCreate, db: Session = Depends(get_db)):
    return trip_repo.create_trip(db=db, trip=trip)

# Get all trips
@router.get("/trips/", response_model=list[trip_schema.TripResponse])
def get_trips(db: Session = Depends(get_db)):
    return trip_repo.get_trips(db)