from fastapi import Depends, APIRouter, HTTPException, status
from sqlalchemy.orm import Session
from database import get_db
from schemas import detail_booking_schema
from repositories import detail_booking_repo
from controllers.auth_ctrl import get_current_user

router = APIRouter()

@router.get("/detail_bookings/", response_model=list[detail_booking_schema.DetailBookingResponse])
def get_detail_bookings(db: Session = Depends(get_db), current_user = Depends(get_current_user)):
    if not current_user:
        raise HTTPException(status_code=status.HTTP_403_FORBIDDEN, detail="Not authorized")
    
    return detail_booking_repo.get_detail_bookings(db)

@router.get("detail_bookings/{select}", response_model=list[detail_booking_schema.DetailBookingResponse])
def get_detail_booking_by(select: str, lookup: str, db: Session = Depends(get_db), current_user = Depends(get_current_user)):
    if not current_user:
        raise HTTPException(status_code=status.HTTP_403_FORBIDDEN, detail="Not authorized")
    
    detail_booking = detail_booking_repo.get_detail_booking_by(db=db, select=select, lookup=lookup)
    if detail_booking == []:
        raise HTTPException(status_code=404, detail="Detail booking not found")
    
    return detail_booking

@router.get("/detail_bookings/full", response_model=detail_booking_schema.DetailBookingResponse)
def get_detail_booking_by_user_booking(idUser: str, idBooking: str, db: Session = Depends(get_db), current_user = Depends(get_current_user)):
    if not current_user:
        raise HTTPException(status_code=status.HTTP_403_FORBIDDEN, detail="Not authorized")
    
    detail_booking = detail_booking_repo.get_detail_booking_by_user_booking(db=db, idUser=idUser, idBooking=idBooking)
    if detail_booking is None:
        raise HTTPException(status_code=404, detail="Detail booking not found")
    
    return detail_booking

@router.post("/detail_bookings/", response_model=detail_booking_schema.DetailBookingResponse)
def create_detail_booking(detail_booking: detail_booking_schema.DetailBookingCreate, db: Session = Depends(get_db), current_user = Depends(get_current_user)):
    if not current_user:
        raise HTTPException(status_code=status.HTTP_403_FORBIDDEN, detail="Not authorized")
    
    return detail_booking_repo.create_detail_booking(db, detail_booking)

@router.delete("/detail_bookings", response_model=detail_booking_schema.DetailBookingResponse)
def delete_detail_booking(idUser: str, idBooking: str, db: Session = Depends(get_db), current_user = Depends(get_current_user)):
    if not current_user:
        raise HTTPException(status_code=status.HTTP_403_FORBIDDEN, detail="Not authorized")
    
    return detail_booking_repo.delete_detail_booking(db=db, idUser=idUser, idBooking=idBooking)