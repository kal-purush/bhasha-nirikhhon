from braille import BrailleTranslator
import pytest


@pytest.fixture
def z_cell():
    t = BrailleTranslator('z')
    # we haz the translator, now get the braille_string for the z-string
    us1 = t.as_grade_1
    # and now get the cell for it. Note that 'z' dots-out like:
    #   * .
    #   . *
    #   * *
    #
    z = us1[0]
    return z


def test_0over_0down_raised(z_cell):
    assert z_cell.raised(0, 0)


def test_0over_1down_not_raised(z_cell):
    assert not z_cell.raised(0, 1)


def test_0over_2down_raised(z_cell):
    assert z_cell.raised(0, 2)


def test_1over_0down_not_raised(z_cell):
    assert not z_cell.raised(1, 0)


def test_1over_1down_raised(z_cell):
    assert z_cell.raised(1, 1)


def test_1over_2down_raised(z_cell):
    assert z_cell.raised(1, 2)