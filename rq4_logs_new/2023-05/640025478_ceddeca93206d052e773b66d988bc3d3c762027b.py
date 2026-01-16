import pytest
from src.keyboard import KeyBoard


@pytest.fixture
def keyboard_fixture():
    return KeyBoard('Dark Project KD87A', 9600, 5)


def test___str__(keyboard_fixture):
    assert str(keyboard_fixture) == "Dark Project KD87A"


def test_language(keyboard_fixture):
    assert str(keyboard_fixture.language) == "EN"


def test_language2(keyboard_fixture):
    keyboard_fixture.change_lang()
    assert str(keyboard_fixture.language) == "RU"


def test_language3(keyboard_fixture):
    keyboard_fixture.change_lang().change_lang()
    assert str(keyboard_fixture.language) == "EN"


def test_language4(keyboard_fixture):
    with pytest.raises(AttributeError):
        keyboard_fixture.language = 'CH'