# -*- coding: utf-8 -*-

from sqlalchemy import Column, DateTime, Integer, ForeignKey
from sqlalchemy.orm import relationship, backref

from DB.dbcore import Base
from .product import Products


class Order(Base):
    """
    Класс Orders, основан на декларативном стиле sqlalchemy, нужен для оформления заказов
    """
    # Инициализация полей таблицы
    __tablename__ = 'orders'
    id = Column(Integer, primary_key=True)
    quantity = Column(Integer)
    data = Column(DateTime)
    product_id = Column(Integer, ForeignKey('products.id'))
    user_id = Column(Integer)
    # используется cascade='delete,all' для каскадного удаления данных из
    # таблицы
    products = relationship(Products,
                            backref=backref('orders',
                                            uselist=True,
                                            cascade='delete,all'))

    def __repr__(self):
        """
        Метод возвращает формальное строковое представление указанного объекта
        """
        return self.quantity