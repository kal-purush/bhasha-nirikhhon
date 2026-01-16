import pytest
import json
from operations.utils import (read_operations, sort_history, encode_in_account,
                              encode_out_account, print_operation)
data = "operations/operations.json"


def test_read_operations():
    operations = read_operations(data)
    assert type(operations) == list


def test_sort_history():
    operations = [
        {
            "id": 441945886,
            "state": "EXECUTED",
            "date": "2019-08-26T10:50:58.294041",
            "operationAmount": {
                "amount": "31957.58",
                "currency": {
                    "name": "руб.",
                    "code": "RUB"
                }
            },
            "description": "Перевод организации",
            "from": "Maestro 1596837868705199",
            "to": "Счет 64686473678894779589"
        },
        {
            "id": 41428829,
            "state": "EXECUTED",
            "date": "2019-07-03T18:35:29.512364",
            "operationAmount": {
                "amount": "8221.37",
                "currency": {
                    "name": "USD",
                    "code": "USD"
                }
            }}
    ]

    sorted_operations = sort_history(operations, 2)

    assert len(sorted_operations) == 2
    assert sorted_operations[0]['id'] == 441945886
    assert sorted_operations[1]['id'] == 41428829


def test_encode_out_account():
    operation = {
        "from": "Maestro 1596837868705199"
        }
    encoded_account = encode_out_account(operation)
    assert encoded_account == "Maestro 1596 83** **** 5199"


def test_print_operation():
    operation = {"id": 114832369, "state": "EXECUTED",
                 "date": "2019-12-07T06:17:14.634890",
                 "operationAmount": {"amount": "48150.39",
                                     "currency": {"name": "USD",
                                                  "code": "USD"
                                                  }
                 },
                 "description": "Перевод организации",
                 "from": "Visa Classic 2842878893689012",
                 "to": "Счет 35158586384610753655"
                 }
    printed_operation = print_operation(operation)

    assert printed_operation == "07.12.2019 Перевод организации\nVisa Classic 2842 87** **** 9012 -> Счет **3655\n48150.39 USD"