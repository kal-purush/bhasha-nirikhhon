from sqlalchemy.orm import Session
from typing import List

from ..models.inspectation import Inspectation

async def add_inspectation(inspectation: Inspectation, db: Session) -> Inspectation:
    db.add(inspectation)
    db.commit()
    db.refresh(inspectation)
    return inspectation


def read_inspectation(inspectation_id: int, db: Session) -> Inspectation:
    inspectation = (
        db.query(Inspectation)
        .filter(Inspectation.inspectation_id == inspectation_id and Inspectation.is_deleted == False)
        .first()
    )
    return inspectation


def soft_delete(inspectation_id: str, db: Session) -> Inspectation:
    inspectation = (
        db.query(Inspectation)
        .where(Inspectation.inspectation_id == inspectation_id and Inspectation.is_deleted == False)
        .first()
    )
    if inspectation:
        inspectation.is_deleted = True
        db.commit()
        db.refresh(inspectation)
        return inspectation
    return None


async def all_inspectations(db: Session) -> List[Inspectation]:
    db_inspectations = db.query(Inspectation).filter(Inspectation.is_deleted == False).all()
    return db_inspectations