import random


def fake_digital_data():
    val = random.getrandbits(1)
    val = int(val)
    return val


def fake_analogue_data():
    val = random.uniform(0, 1)
    return val


val_analogue = fake_analogue_data()
case_list_test_analogue = {"UI1": {"val": val_analogue}, "UI2": {"val": val_analogue},
             "UI3": {"val": val_analogue}, "UI4": {"val": val_analogue}, "UI5": {"val": val_analogue},
             "UI6": {"val": val_analogue}, "UI7": {"val": val_analogue}},

val_digital = fake_digital_data()
case_list_test_digital = {"DI1": {"val": val_digital}, "DI2": {"val": val_digital}, "DI3": {"val": val_digital}, "DI4": {"val": val_digital}, "DI5": {"val": val_digital},
             "DI6": {"val": val_digital}, "DI7": {"val": val_digital}}