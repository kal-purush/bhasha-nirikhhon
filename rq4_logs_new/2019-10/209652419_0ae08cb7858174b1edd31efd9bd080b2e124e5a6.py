# -*- coding: utf-8 -*-

from sqlalchemy import Column, String, Integer, Boolean

from .dbcore import Base


class Category(Base):
    """
    Класс для создания таблицы Category товара, основан на декларативном
    стиле sqlalchemy

    """
    # Инициализация полей таблицы
    __tablename__ = 'category'
    id = Column(Integer, primary_key=True)
    name = Column(String, index=True)
    is_active = Column(Boolean)

    def __repr__(self):
        """
        Метод возвращает формальное строковое представление указанного объекта

        """
        return self.name