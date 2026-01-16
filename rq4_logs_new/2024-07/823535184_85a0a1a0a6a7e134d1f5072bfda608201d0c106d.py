import pytest
import json
from main import find_translation, private, return_translation, lust_translations

# Тестовые данные
test_data = [
    {
        "date": "2023-07-01T12:34:56.000",
        "state": "EXECUTED",
        "operationAmount": {
            "amount": "1000.00",
            "currency": {
                "name": "USD"
            }
        },
        "description": "Payment",
        "from": "Card 1234567812345678",
        "to": "Card 8765432187654321"
    },
    {
        "date": "2023-07-02T12:34:56.000",
        "state": "EXECUTED",
        "operationAmount": {
            "amount": "2000.00",
            "currency": {
                "name": "EUR"
            }
        },
        "description": "Transfer",
        "from": "Счет 1234567890",
        "to": "Card 8765432187654321"
    },
    {
        "date": "2023-07-03T12:34:56.000",
        "state": "CANCELLED",
        "operationAmount": {
            "amount": "3000.00",
            "currency": {
                "name": "RUB"
            }
        },
        "description": "Refund",
        "from": "Card 8765432187654321",
        "to": "Счет 1234567890"
    }
]

# Запись тестовых данных в файл
with open('test_operations.json', 'w') as f:
    json.dump(test_data, f)


def test_find_translation():
    data = find_translation(lust_translations('test_operations.json'))
    assert data == test_data


def test_private():
    result_from = private(test_data[0], "from")
    assert result_from == "Card 1234 56** **** 5678"
    result_to = private(test_data[0], "to")
    assert result_to == "Card 8765 43** **** 4321"


def test_return_translation():
    result = return_translation(test_data[0])
    assert "01.07.2023 Payment" in result
    assert "Card 1234 56** **** 5678 -> Card 8765 43** **** 4321" in result
    assert "1000.00 USD" in result


if __name__ == "__main__":
    pytest.main()