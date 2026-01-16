# -*- coding: utf-8 -*-
"""Test cases for Calculators.

- Author: Hyungseok Shin
- Contact: hsshin@jmarple.ai
"""

import pytest

from src.calculator import Adder, Calculator, Divider, Multiplier, Subtractor


class TestCalculator:
    def test_one(self):
        with pytest.raises(NotImplementedError):
            assert Calculator().operate(0, 0)


class TestAdder:
    def test_one(self):
        assert Adder().operate(0, 0) == 0

    def test_two(self):
        assert Adder().operate(-1, 2) == 1


class TestSubtractor:
    def test_one(self):
        assert Subtractor().operate(0, 0) == 0

    def test_two(self):
        assert Subtractor().operate(-1, 2) == -3

    def test_three(self):
        assert Subtractor().operate(2, 1) == 1


class TestMultiplier:
    def test_one(self):
        assert Multiplier().operate(1, 0) == 0

    def test_two(self):
        assert Multiplier().operate(2, 3) == 6

    def test_three(self):
        assert Multiplier().operate(-2, 1) == -2

    def test_four(self):
        assert Multiplier().operate(-1, -2) == 2


class TestDivider:
    def test_one(self):
        assert Divider().operate(2, 1) == 2

    def test_two(self):
        assert Divider().operate(3, 2) == 1

    def test_three(self):
        assert Divider().operate(-2, 4) == -1

    def test_four(self):
        assert Divider().operate(3, -2) == -2

    def test_five(self):
        with pytest.raises(ZeroDivisionError):
            Divider().operate(2, 0)