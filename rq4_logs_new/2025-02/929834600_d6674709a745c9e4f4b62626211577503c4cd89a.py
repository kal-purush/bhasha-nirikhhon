import pytest
from main import calculate_and_print

@pytest.mark.parametrize("x_string, y_string, operation_string, expected_string", [
    ("3", "3", 'add', "The result of 3 add 3 is equal to 6"),
    ("4", "1", 'subtract', "The result of 4 subtract 1 is equal to 3"),
    ("2", "2", 'multiply', "The result of 2 multiply by 2 is equal to 4"),
    ("10", "5", 'divide', "The result of 10 divide by 5 is equal to 2"),
    ("4", "0", 'divide', "An error occurred: Cannot divide by zero"),  
    ("6", "3", 'unknown', "Unknown operation: unknown"),  
    ("x", "3", 'add', "Invalid number input: x or 3 is not a valid number."),  
    ("5", "y", 'subtract', "Invalid number input: 5 or y is not a valid number.")  
])
def test_calculate_and_print(x_string, y_string, operation_string,expected_string, capsys):
    calculate_and_print(x_string, y_string, operation_string)
    captured = capsys.readouterr()
    assert captured.out.strip() == expected_string