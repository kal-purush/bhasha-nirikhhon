from sqlalchemy.orm import Session
from pizza_project import models, schemas


def create_pizza(db: Session, pizza: schemas.PizzaCreate):
    db_pizza = models.Pizza(
        name=pizza.name,
        description=pizza.description,
        size=pizza.size,
        price=pizza.price)
    db.add(db_pizza)
    db.commit()
    db.refresh(db_pizza)
    return db_pizza


def get_pizza(db: Session, skip: int = 0, limit: int = 100):
    pizza = db.query(models.Pizza).offset(skip).limit(limit).all()
    return pizza