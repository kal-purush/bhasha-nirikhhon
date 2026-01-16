from sqlalchemy.ext.declarative import declarative_base


class Base:
    base = None

    @staticmethod
    def getBase():
        if Base.base == None:
            Base.base = declarative_base()
            return Base.base
        else:
            return Base.base