import sqlalchemy as db
from sqlalchemy.orm import sessionmaker, declarative_base, validates
from sqlalchemy_utils import drop_database, create_database, database_exists
from sqlalchemy import event, DDL, ForeignKey

db_name = 'trigger'
db_type = 'mysql'
device = 'pymysql'
user = 'root'
host = '127.0.0.1'
port = '3306'
sock = '/tmp/mysql.sock'
engine = db.create_engine(
    f"{db_type}+{device}://{user}:@{host}:{port}/{db_name}?unix_socket={sock}"
)

if database_exists(engine.url):
    drop_database(engine.url)
if not database_exists(engine.url):
    create_database(engine.url)

base = declarative_base()

class Transactions(base):
    __tablename__ = 'trans'
    id = db.Column(db.Integer(), primary_key=True, autoincrement=True)
    item = db.Column(db.String(20))
    action = db.Column(db.Float())
    no_units = db.Column(db.Float())
    at_price = db.Column(db.Float())
    def __init__(self, item, action, no_units, at_price):
        self.item = item
        self.action = action
        self.no_units = no_units
        self.at_price = at_price 

class Inventory(base):
    __tablename__ = 'invetory'
    item = db.Column(
        db.String(20), 
        primary_key=True, 
    )
    inv = db.Column(db.Integer())
    current_value = db.Column(db.Float())
    profit = db.Column(db.Float())
    gain = db.Column(db.Float())

base.metadata.create_all(bind=engine)

def trigger():

    query = 'create trigger update_inv '
    query += 'after insert on trans '
    query += 'for each row '
    query += 'begin '
    query += 'INSERT INTO invetory '
    query += 'values (new.item, '
    query += 'new.no_units * new.action, '
    query += 'new.at_price * inv, '
    query += '-1 * new.at_price * new.no_units * new.action, '
    query += '0) '
    query += 'ON DUPLICATE KEY UPDATE '
    query += 'inv = inv + new.no_units * new.action, '
    query += 'profit = profit - (new.no_units * new.at_price * new.action), '
    query += 'current_value = new.at_price * (inv), '
    query += 'gain = profit + current_value / f.totalspent '
    query += 'from( '
    query += 'select item, sum(at_price) as totalspent from invetory '
    query += 'group by item '
    query += ') as f '
    query += 'where f.item = new.item; '
    query += 'end;'
    return query

with engine.connect() as conn:
    conn.execute(
        db.text(trigger())
    )
    conn.commit()

session = sessionmaker(bind=engine)()
transactions = [
    ['car', 1, 3, 50],
    ['car', -1, 2, 100],
    ['boat', 1, 5, 20],
    ['boat', -1, 3, 10],
]
for t in transactions:
    trans = Transactions(*t)
    session.add(trans)
    session.commit()