from app.models import db, Notebook, environment, SCHEMA
from sqlalchemy.sql import text

def seed_notebooks():
    chem = Notebook(
        user_id=2, name="Intro to Chemistry"
    )
    forensic = Notebook(
        user_id=2, name="Intro to Forensics"
    )
    pastries = Notebook(
        user_id=3, name="Pastry Ideas"
    )
    recipes = Notebook(
        user_id=3, name="Recipes"
    )
    demo = Notebook(
        user_id=1, name="Demo Notebook"
    )

    db.session.add_all([chem, forensic, pastries, recipes, demo])
    db.session.commit()


def undo_notebooks():
    if environment == "production":
        db.session.execute(
            f"TRUNCATE table {SCHEMA}.notebooks RESTART IDENTITY CASCADE;"
        )
    else:
        db.session.execute(text("DELETE FROM notebooks"))

    db.session.commit()