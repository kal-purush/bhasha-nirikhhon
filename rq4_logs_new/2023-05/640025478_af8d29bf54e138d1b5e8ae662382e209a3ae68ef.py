import pytest
from src.phone import Phone
from src.item import Item


@pytest.fixture
def phone_fixture():
    return Phone("iPhone 14", 120_000, 5, 2)


@pytest.fixture
def item_fixture():
    return Item("Смартфон", 10000, 20)


def test___repr__(phone_fixture):
    assert repr(phone_fixture) == "Phone('iPhone 14', 120000, 5, 2)"


def test___str__(phone_fixture):
    assert str(phone_fixture) == 'iPhone 14'


def test___add__(phone_fixture):
    assert phone_fixture + phone_fixture == 10


def test___add__2(phone_fixture, item_fixture):
    assert phone_fixture + item_fixture == 25


def test___add__typeerror(phone_fixture):
    with pytest.raises(TypeError):
        phone_fixture + 10


def test_number_of_sim(phone_fixture):
    assert phone_fixture.number_of_sim == 2


def test_number_of_sim_2(phone_fixture):
    phone_fixture.number_of_sim = 1
    assert phone_fixture.number_of_sim == 1


def test_number_of_sim_3(phone_fixture):
    with pytest.raises(ValueError):
        phone_fixture.number_of_sim = 0


def test_number_of_sim_4(phone_fixture):
    with pytest.raises(ValueError):
        phone_fixture.number_of_sim = 1.5