import sqlalchemy

engine = sqlalchemy.create_engine("mysql+pymysql://root:abcd=1234@localhost:3306/itcast")

meta_data = sqlalchemy.MetaData()

department = sqlalchemy.Table(
    "department", meta_data,
    sqlalchemy.Column("id", sqlalchemy.Integer, primary_key=True),
    sqlalchemy.Column("name", sqlalchemy.String(255), nullable=False, unique=True),
)

employee = sqlalchemy.Table(
    "employee", meta_data,
    sqlalchemy.Column("id", sqlalchemy.Integer, primary_key=True),
    sqlalchemy.Column("department_id", sqlalchemy.Integer, sqlalchemy.ForeignKey("department.id"), nullable=False),
    sqlalchemy.Column("name", sqlalchemy.String(255), nullable=False),
)

meta_data.create_all(engine)