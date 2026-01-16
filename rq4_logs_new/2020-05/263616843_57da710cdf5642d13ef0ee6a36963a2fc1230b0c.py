from sqlalchemy import create_engine
from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy import Column, Integer, String, Date
from sqlalchemy.orm import sessionmaker
from datetime import datetime
 
engine = create_engine('sqlite:///list.db')
Base = declarative_base()
 
 
class Table(Base):
    __tablename__ = 'task'
    id = Column(Integer, primary_key=True)
    task = Column(String)
    deadline = Column(Date, default=datetime.today())
 
    def __repr__(self):
        return self.task
 
 
Base.metadata.create_all(engine)
Session = sessionmaker(bind=engine)
session = Session()
menu_text = '''
1) Today's tasks
2) Add task
0) Exit
'''
 
while True:
    rows = session.query(Table).all()
    command = input(menu_text)
    if command == '0':
        print('Bye!')
        break
    elif command == '1':
        print('Today:')
        if len(rows) == 0:
            print('Nothing to do!')
        else:
            counter = 1
            for row in rows:
                print(f'{counter}. {row}')
                counter += 1
    elif command == '2':
        task = input('Enter task\n')
        new_row = Table(task=task)
        session.add(new_row)
        session.commit()
        print('The task has been added!')