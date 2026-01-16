from sqlalchemy.orm import Session
from models import User

# insert new record into the database
def create_user(db: Session, name: str, email: str) -> User:
    db_user = User(name=name, email=email)
    db.add(db_user)
    db.commit()
    db.refresh(db_user)
    return db_user

# get single record from the database by id
def get_user(db: Session, user_id: int) -> User | None:
    return db.query(User).filter(User.id == user_id).first()

# get all records from the database
def get_users(db: Session, skip: int = 0, limit: int = 10) -> list[User]:
    return db.query(User).offset(skip).limit(limit).all()

# update user record in the database
def update_user(db: Session, user_id: int, name: str | None, email: str | None) -> User | None:
    db_user = db.query(User).filter(User.id == user_id).first()
    if not db_user:
        return None
    if name:
        db_user.name = name
    if email:
        db_user.email = email
    db.commit()
    db.refresh(db_user)
    return db_user


# delete record from the database
def delete_user(db: Session, user_id: int) -> bool:
    db_user = db.query(User).filter(User.id == user_id).first()
    if not db_user:
        return False
    db.delete(db_user)
    db.commit()
    return True