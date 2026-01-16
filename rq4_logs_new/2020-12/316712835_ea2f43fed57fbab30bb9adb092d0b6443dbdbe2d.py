from fastapi import APIRouter, Depends, HTTPException

from dependencies import get_token_header

from typing import List
from fastapi import Body, status

from fastapi.responses import JSONResponse, Response
from sqlalchemy.orm import Session
from repository import crud, models, schemas
from routers import participants
from repository.database import SessionLocal, engine

from fastapi.encoders import jsonable_encoder
from fastapi.responses import JSONResponse

router = APIRouter()
# router = APIRouter( prefix="/draws", tags=["draws"], dependencies=[Depends(get_token_header)], responses={404: {"description": "Not found"}})

def get_db():
    db = SessionLocal()
    try:
        yield db
    finally:
        db.close()


# ###################################################################### #
# these operations are for running draws                                 #
# ###################################################################### #

# get access to a draw:
@router.post("/login", response_model=schemas.DrawParticipants)
def get_access_draw(participant: str, access_code: str, db: Session = Depends(get_db)):
    print('find a participant in a draw using {}:{}'.format(participant, access_code))
    if '@' in participant: # maybe is a email, so I let it in
        access = crud.get_access(db=db, participant=participant, access_code=access_code)
        print('access result:', access)
        if access:
            return access
        else:
            error = {"code": "RESOURCE_NOT_FOUND", "message": "The draw event resource isn't started or doesn't exists. Verify the access code {}".format(access_code) }
            json_compatible_error_data = jsonable_encoder(error)
            return JSONResponse(status_code=404, content=json_compatible_error_data)
    else:
        error = {"code": "UNPROCESABLE_ENTITY", "message": "The params used are not valid for this operation." }
        json_compatible_error_data = jsonable_encoder(error)
        return JSONResponse(status_code=422, content=json_compatible_error_data)


@router.get("/{drawid}/selections", response_model=schemas.DrawGiftsAvailable)
def get_available_gits(drawid: int, participantid: int, db: Session = Depends(get_db)):
    print('finding available gifts for {}'.format(participantid))
    participant = crud.get_participant(db=db, participant_id=participantid)
    print('participant found:', participant)
    if participant:
        avilables = crud.get_available_gifts(db=db, iddraw=drawid, idgroup=participant.group.id)
        return avilables
    else:
        error = {"code": "RESOURCE_NOT_FOUND", "message": "There are not available gifts for these params." }
        json_compatible_error_data = jsonable_encoder(error)
        return JSONResponse(status_code=404, content=json_compatible_error_data)

@router.post("/{drawid}/selections", status_code=201)
def post_draw_participant_gift(drawid: int, alias: str, participantid: int, db: Session = Depends(get_db)):    
    print('Selecting a gift for: ', participantid, alias)
    draw_participant_gift = crud.add_draw_participant_gift(db=db, drawid=drawid, alias=alias, participantid=participantid)
    return draw_participant_gift

@router.get("/{drawid}/selections/participants/{participantid}", status_code=200)
def get_selection_participant(drawid: int, participantid: int, db: Session = Depends(get_db)):
    print("looking the gift for participant:", participantid)
    selected = crud.get_draw_participant_gift(db=db, draw_id=drawid, participant_id=participantid)
    if selected:
        print('record retrieved:', selected)
        return selected
    else:
        error = {"code": "RESOURCE_NOT_FOUND", "message": "There are not selected gifts for these participant." }
        json_compatible_error_data = jsonable_encoder(error)
        return JSONResponse(status_code=404, content=json_compatible_error_data)

@router.get("/{drawid}/selections/participants/", status_code=200)
def get_selection_participants(drawid: int, db: Session = Depends(get_db)):
    print("looking the gift for draw:", drawid)
    selected = crud.get_draw_participants_gifts(db=db, draw_id=drawid)
    if selected:
        print('records retrieved:', selected)
        return selected
    else:
        error = {"code": "RESOURCE_NOT_FOUND", "message": "There are not selected gifts for these participant." }
        json_compatible_error_data = jsonable_encoder(error)
        return JSONResponse(status_code=404, content=json_compatible_error_data)