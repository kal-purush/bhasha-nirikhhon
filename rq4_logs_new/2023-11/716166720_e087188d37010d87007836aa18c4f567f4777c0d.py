from sqlalchemy.orm import Session
import models
import schemas

def create_user(db: Session, user: schemas.UserCreate):
    db_user = models.User(**user.dict())
    db.add(db_user)
    db.commit()
    db.refresh(db_user)
    return db_user

def create_character(db: Session, character: schemas.CharacterCreate):
    db_character = models.Character(**character.dict())
    db.add(db_character)
    db.commit()
    db.refresh(db_character)
    return db_character

def create_character_stats(db: Session, character: schemas.CharacterStatsCreate):
    db_character_stats = models.Character_stats(**character.dict())
    db.add(db_character_stats)
    db.commit()
    db.refresh(db_character_stats)
    return db_character_stats

def get_user(db: Session, user_id: int):
    return db.query(models.User).filter(models.User.id == user_id).first()

def get_user_by_name(db: Session, user_name: str):
    return db.query(models.User).filter(models.User.user_name == user_name).first()

def get_user_by_email(db: Session, email: str):
    return db.query(models.User).filter(models.User.email == email).first()

def get_character(db: Session, character_id: int):
    return db.query(models.Character).filter(models.Character.id == character_id).first()

def get_character_by_name(db: Session, character_name: str):
    return db.query(models.Character).filter(models.Character.character_name == character_name).first()

def get_character_stats(db: Session, character_stats_id: int):
    return db.query(models.Character_stats).filter(models.Character_stats.id == character_stats_id).first()

def delete_user(db: Session, user_id: int):
    db_user = db.query(models.User).filter(models.User.id == user_id).first()
    if db_user:
        db.delete(db_user)
        db.commit()
        return db_user
    return None

def delete_character(db: Session, character_id: int):
    db_character = db.query(models.Character).filter(models.Character.id == character_id).first()
    if db_character:
        db.delete(db_character)
        db.commit()
        return db_character
    return None

def delete_character_stats(db: Session, character_stats_id: int):
    db_character_stats = db.query(models.Character_stats).filter(models.Character_stats.id == character_stats_id).first()
    if db_character_stats:
        db.delete(db_character_stats)
        db.commit()
        return db_character_stats
    return None