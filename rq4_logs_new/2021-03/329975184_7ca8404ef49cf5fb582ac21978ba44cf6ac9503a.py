from fastapi.testclient import TestClient
from sqlalchemy import create_engine
from sqlalchemy.orm import sessionmaker

from database import Base, get_db
from main import app 

db_url='localhost'
db_name='moody_testing'
db_username='moody'
db_password=''

SQLALCHEMY_DATABASE_URL = f'postgresql://{db_username}:{db_password}@{db_url}/{db_name}'

engine = create_engine(
    SQLALCHEMY_DATABASE_URL, connect_args={}
)

TestingSessionLocal = sessionmaker(autocommit=False, autoflush=False, bind=engine)

Base.metadata.create_all(bind=engine)

def override_get_db():
    try:
        db = TestingSessionLocal()
        yield db
    finally:
        db.close()

app.dependency_overrides[get_db] = override_get_db

client = TestClient(app)

def test_root():
    response = client.get('/')
    assert response.status_code == 200
    assert response.json() == {'message': 'Hello World'}

# def test_create_user():
    # response = client.post(
        # "/users/",
        # json={"email": "deadpool@example.com", "password": "chimichangas4life"},
    # )
    # assert response.status_code == 200, response.text
    # data = response.json()
    # assert data["email"] == "deadpool@example.com"
    # assert "id" in data
    # user_id = data["id"]

    # response = client.get(f"/users/{user_id}")
    # assert response.status_code == 200, response.text
    # data = response.json()
    # assert data["email"] == "deadpool@example.com"
    # assert data["id"] == user_id