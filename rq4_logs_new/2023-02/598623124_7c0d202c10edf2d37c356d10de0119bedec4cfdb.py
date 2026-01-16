from sqlalchemy import create_engine
from sqlalchemy.orm import DeclarativeBase
from sqlalchemy.orm import Mapped, mapped_column
from sqlalchemy.orm import Session

class Base(DeclarativeBase):
    pass

class Student(Base):
    __tablename__ = "Students" 
    ID : Mapped[int] = mapped_column(primary_key=True, autoincrement=True)
    FirstName : Mapped[str] = mapped_column()
    LastName : Mapped[str] = mapped_column()
    Rating : Mapped[int] = mapped_column()
    def __repr__(self) -> str:
        return f"Student(ID={self.ID!r}, FirstName={self.FirstName!r}, LastName={self.LastName!r}, Rating={self.Rating!r})"



engine = create_engine("sqlite:///db.db")
Base.metadata.drop_all(engine)
Base.metadata.create_all(engine)

with Session(engine) as session:
    Student1 = Student(FirstName="Student1", LastName="1850", Rating=1)
    Student2 = Student(FirstName="Student2", LastName="1850", Rating=2)
    Student3 = Student(FirstName="Alexey", LastName="Lozovoy", Rating=3)
    Student4 = Student(FirstName="Student4", LastName="1850", Rating=4)
    Student5 = Student(FirstName="Student5", LastName="1850", Rating=5)
    Student6 = Student(FirstName="Student6", LastName="1850", Rating=6)
    Student7 = Student(FirstName="Student7", LastName="1850", Rating=7)

    session.add_all([Student1, Student2, Student3, Student4, Student5, Student6, Student7])
    session.commit()

    Student3.Rating = Student3.Rating +2
    session.commit()

    SR = session.query(Student).filter(Student.Rating > Student3.Rating).all()
    print(SR)

    session.query(Student).filter(Student.Rating < 5).delete()
    session.commit()
    

