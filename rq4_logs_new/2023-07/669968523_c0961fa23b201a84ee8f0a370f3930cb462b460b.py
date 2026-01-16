import pytest
from fastapi.testclient import TestClient
from sqlalchemy import create_engine
from sqlalchemy.orm import sessionmaker

from db.database import DATABASE_URL, Base
from db.models import Dish, Menu, Submenu
from main import app

HOST = "http://127.0.0.1:8000/api/v1/"


test_engine = create_engine(DATABASE_URL)

TestSession = sessionmaker(autocommit=False, autoflush=False, bind=test_engine)


@pytest.fixture
def test_client():
    client = TestClient(app)
    yield client


@pytest.fixture
def test_session():
    session = TestSession()
    try:
        yield session
    finally:
        session.close()


def get_menu_obj(test_session):
    return test_session.query(Menu).first()


def get_submenu_obj(test_session):
    return test_session.query(Submenu).first()


def get_dish_obj(test_session):
    return test_session.query(Dish).first()


def test_clean_table():
    Base.metadata.drop_all(test_engine)
    Base.metadata.create_all(test_engine)


def test_empty_menus(test_client):
    response = test_client.get(f"{HOST}menus/")
    assert response.status_code == 200
    assert response.json() == []


def test_create_menu(test_client, test_session):
    request_test_menu = {
        "title": "My menu 1",
        "description": "My menu description 1",
    }
    response = test_client.post(f"{HOST}menus/", json=request_test_menu)
    assert response.status_code == 201
    menu = get_menu_obj(test_session)
    assert menu is not None
    response_test_menu = {
        "title": "My menu 1",
        "description": "My menu description 1",
        "id": str(menu.id),
        "submenus_count": 0,
        "dishes_count": 0,
    }
    assert response.json() == response_test_menu


def test_empty_submenus(test_client, test_session):
    menu = get_menu_obj(test_session)
    response = test_client.get(f"{HOST}menus/{menu.id}/submenus/")
    assert response.status_code == 200
    assert response.json() == []


def test_create_submenu(test_client, test_session):
    menu = get_menu_obj(test_session)
    request_test_submenu = {
        "title": "My submenu 1",
        "description": "My submenu description 1",
    }
    response = test_client.post(
        f"{HOST}menus/{menu.id}/submenus/", json=request_test_submenu
    )
    assert response.status_code == 201
    submenu = get_submenu_obj(test_session)
    response_test_submenu = {
        "title": "My submenu 1",
        "description": "My submenu description 1",
        "id": str(submenu.id),
        "dishes_count": 0,
    }
    assert response.json() == response_test_submenu


def test_empty_dishes(test_client, test_session):
    menu = get_menu_obj(test_session)
    submenu = get_submenu_obj(test_session)
    response = test_client.get(
        f"{HOST}menus/{menu.id}/submenus/{submenu.id}/dishes/"
    )
    assert response.status_code == 200
    assert response.json() == []


def test_create_dish(test_client, test_session):
    menu = get_menu_obj(test_session)
    submenu = get_submenu_obj(test_session)
    request_test_dish = {
        "title": "My dish 1",
        "description": "My dish description 1",
        "price": 12.5,
    }
    response = test_client.post(
        f"{HOST}menus/{menu.id}/submenus/{submenu.id}/dishes/",
        json=request_test_dish,
    )
    assert response.status_code == 201
    dish = get_dish_obj(test_session)
    response_test_dish = {
        "title": "My dish 1",
        "description": "My dish description 1",
        "price": "12.5",
        "id": str(dish.id),
    }
    assert response.json() == response_test_dish


def test_get_dish(test_client, test_session):
    menu = get_menu_obj(test_session)
    submenu = get_submenu_obj(test_session)
    dish = get_dish_obj(test_session)
    response = test_client.get(
        f"{HOST}menus/{menu.id}/submenus/{submenu.id}/dishes/{dish.id}/"
    )
    response_test_dish = {
        "title": "My dish 1",
        "description": "My dish description 1",
        "price": "12.5",
        "id": str(dish.id),
    }
    assert response.json() == response_test_dish


def test_update_dish(test_client, test_session):
    menu = get_menu_obj(test_session)
    submenu = get_submenu_obj(test_session)
    dish = get_dish_obj(test_session)
    request_test_dish = {
        "title": "My updated dish 1",
        "description": "My updated dish description 1",
        "price": 14.5,
    }
    response = test_client.patch(
        f"{HOST}menus/{menu.id}/submenus/{submenu.id}/dishes/{dish.id}/",
        json=request_test_dish,
    )
    assert response.status_code == 200
    response_test_dish = {
        "title": "My updated dish 1",
        "description": "My updated dish description 1",
        "price": "14.5",
        "id": str(dish.id),
    }
    assert response.json() == response_test_dish


def test_get_submenu(test_client, test_session):
    menu = get_menu_obj(test_session)
    submenu = get_submenu_obj(test_session)
    response = test_client.get(f"{HOST}menus/{menu.id}/submenus/{submenu.id}/")
    assert response.status_code == 200
    response_test_submenu = {
        "title": "My submenu 1",
        "description": "My submenu description 1",
        "id": str(submenu.id),
        "dishes_count": 1,
    }
    assert response.json() == response_test_submenu


def test_update_submenu(test_client, test_session):
    menu = get_menu_obj(test_session)
    submenu = get_submenu_obj(test_session)
    request_test_submenu = {
        "title": "My updated submenu 1",
        "description": "My updated submenu description 1",
    }
    response = test_client.patch(
        f"{HOST}menus/{menu.id}/submenus/{submenu.id}/",
        json=request_test_submenu,
    )
    assert response.status_code == 200
    response_test_submenu = {
        "title": "My updated submenu 1",
        "description": "My updated submenu description 1",
        "id": str(submenu.id),
        "dishes_count": 1,
    }
    assert response.json() == response_test_submenu


def test_get_menu(test_client, test_session):
    menu = get_menu_obj(test_session)
    response = test_client.get(f"{HOST}menus/{menu.id}/")
    assert response.status_code == 200
    response_test_menu = {
        "title": "My menu 1",
        "description": "My menu description 1",
        "id": str(menu.id),
        "submenus_count": 1,
        "dishes_count": 1,
    }
    assert response.json() == response_test_menu


def test_update_menu(test_client, test_session):
    menu = get_menu_obj(test_session)
    request_test_menu = {
        "title": "My updated menu 1",
        "description": "My updated menu description 1",
    }
    response = test_client.patch(
        f"{HOST}menus/{menu.id}/", json=request_test_menu
    )
    assert response.status_code == 200
    response_test_menu = {
        "title": "My updated menu 1",
        "description": "My updated menu description 1",
        "id": str(menu.id),
        "submenus_count": 1,
        "dishes_count": 1,
    }
    assert response.json() == response_test_menu


def test_delete_dish(test_client, test_session):
    menu = get_menu_obj(test_session)
    submenu = get_submenu_obj(test_session)
    dish = get_dish_obj(test_session)
    response = test_client.delete(
        f"{HOST}menus/{menu.id}/submenus/{submenu.id}/dishes/{dish.id}/"
    )
    assert response.status_code == 200
    assert response.json() == {
        "status": True,
        "message": "The dish has been deleted",
    }


def test_get_submenu_count(test_client, test_session):
    menu = get_menu_obj(test_session)
    submenu = get_submenu_obj(test_session)
    response = test_client.get(f"{HOST}menus/{menu.id}/submenus/{submenu.id}/")
    assert response.status_code == 200
    response_test_submenu = {
        "title": "My updated submenu 1",
        "description": "My updated submenu description 1",
        "id": str(submenu.id),
        "dishes_count": 0,
    }
    assert response.json() == response_test_submenu


def test_get_menu_count(test_client, test_session):
    menu = get_menu_obj(test_session)
    response = test_client.get(f"{HOST}menus/{menu.id}/")
    assert response.status_code == 200
    response_test_menu = {
        "title": "My updated menu 1",
        "description": "My updated menu description 1",
        "id": str(menu.id),
        "submenus_count": 1,
        "dishes_count": 0,
    }
    assert response.json() == response_test_menu


def test_delete_submenu(test_client, test_session):
    menu = get_menu_obj(test_session)
    submenu = get_submenu_obj(test_session)
    response = test_client.delete(
        f"{HOST}menus/{menu.id}/submenus/{submenu.id}/"
    )
    assert response.status_code == 200
    assert response.json() == {
        "status": True,
        "message": "The submenu has been deleted",
    }


def test_get_menu_count2(test_client, test_session):
    menu = get_menu_obj(test_session)
    response = test_client.get(f"{HOST}menus/{menu.id}/")
    assert response.status_code == 200
    response_test_menu = {
        "title": "My updated menu 1",
        "description": "My updated menu description 1",
        "id": str(menu.id),
        "submenus_count": 0,
        "dishes_count": 0,
    }
    assert response.json() == response_test_menu


def test_delete_menu(test_client, test_session):
    menu = get_menu_obj(test_session)
    response = test_client.delete(f"{HOST}menus/{menu.id}/")
    assert response.status_code == 200
    assert response.json() == {
        "status": True,
        "message": "The menu has been deleted",
    }