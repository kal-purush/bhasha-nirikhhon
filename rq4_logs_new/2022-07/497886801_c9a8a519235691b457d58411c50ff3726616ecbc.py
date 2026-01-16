from fastapi import Depends, HTTPException, APIRouter
from sqlalchemy.orm import Session
from typing import List
from fastapi.security import OAuth2PasswordBearer
from utils.get_current_db import get_db
from database.enums.TransportType import TransportType
from schemas.MotoSchema import MotoSchema
from schemas.TransportSchema import TransportSchema
from service.TransportService import (
    get_transport,
    get_transport_by_id,
    create_transport,
)

oauth2_scheme = OAuth2PasswordBearer(tokenUrl="login")
moto_router = APIRouter()


@moto_router.get("/moto", response_model=List[MotoSchema])
def get(
    db: Session = Depends(get_db),
    args: TransportSchema = Depends(),
):
    return get_transport(
        db,
        args=args,
        type=TransportType['MOTO'],
    )


@moto_router.get("/moto/{id}", response_model=MotoSchema)
def get_one(
        id: int,
        db: Session = Depends(get_db),
):
    moto = get_transport_by_id(db, id=id, type=TransportType['MOTO'])
    if moto is None:
        raise HTTPException(status_code=404, detail="Moto not found")
    return moto


@moto_router.post("/moto")
def create_moto(
        item: MotoSchema,
        db: Session = Depends(get_db),
):
    return create_transport(db=db, item=item, type=TransportType['MOTO'])