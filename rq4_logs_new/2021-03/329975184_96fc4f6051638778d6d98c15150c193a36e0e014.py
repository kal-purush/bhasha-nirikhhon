from sqlalchemy.orm import Session
from . import JournalEntryBase
import models

class JournalEntryRepository():
    @staticmethod
    def get_journal_entries(db: Session, skip: int = 0, limit: int = 100): 
        return db.query(models.JournalEntry).offset(skip).limit(limit).all()

    @staticmethod
    def get_journal_entries_by_user(db: Session, user_id: int):
        return db.query(models.JournalEntry).filter(models.JournalEntry.user_id == user_id).all()

    @staticmethod
    def create_journal_entry(db: Session, journal_entry: JournalEntryBase, user_id: int):
        db_journal_entry = models.JournalEntry(**journal_entry.dict(), user_id = user_id)
        db.add(db_journal_entry)
        db.commit()
        db.refresh(db_journal_entry)
        return db_journal_entry