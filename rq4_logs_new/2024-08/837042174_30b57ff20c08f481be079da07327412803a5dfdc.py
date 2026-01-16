"""Includes test cases for all the functions at gos/loggers"""

from unittest.mock import patch
from colorama import Fore, Style
from gos.loggers import (
    print_stuff,
    print_error,
    print_success,
    print_divider,
)

############ print_stuff ############
# 3 valid case


def test_print_stuff_when_not_silent_valid():
    """Test print_stuff when not silent and valid"""
    message = "Hello, World!"
    color = Fore.RED
    expected_output = color + message + Style.RESET_ALL

    with patch("builtins.print") as mock_print:
        print_stuff(message, is_silent=False, color=color)
        mock_print.assert_called_once_with(expected_output)


def test_print_stuff_when_silent_valid():
    """Test print_stuff when silent and valid"""
    message = "Hello, World!"
    color = Fore.RED

    with patch("builtins.print") as mock_print:
        print_stuff(message, is_silent=True, color=color)
        mock_print.assert_not_called()


def test_print_stuff_when_no_color_valid():
    """Test print_stuff when no color and valid"""
    message = "Hello, World!"
    color = Fore.WHITE
    expected_output = color + message + Style.RESET_ALL

    with patch("builtins.print") as mock_print:
        print_stuff(message, is_silent=False)
        mock_print.assert_called_once_with(expected_output)


############ print_error ############
# 2 valid cases


def test_print_error_when_not_silent_valid():
    """Test print_error when not silent and valid"""
    message = "Error: Something went wrong"
    color = Fore.RED
    expected_output = color + f"✗ {message}" + Style.RESET_ALL

    with patch("builtins.print") as mock_print:
        print_error(message, is_silent=False)
        mock_print.assert_called_once_with(expected_output)


def test_print_error_when_silent_valid():
    """Test print_error when silent and valid"""
    message = "Error: Something went wrong"

    with patch("builtins.print") as mock_print:
        print_error(message, is_silent=True)
        mock_print.assert_not_called()


############ print_success ############
# 2 valid cases


def test_print_success_when_not_silent_valid():
    """Test print_error when not silent and valid"""
    message = "Success: Input is valid"
    color = Fore.GREEN
    expected_output = color + f"✔ {message}" + Style.RESET_ALL

    with patch("builtins.print") as mock_print:
        print_success(message, is_silent=False)
        mock_print.assert_called_once_with(expected_output)


def test_print_success_when_silent_valid():
    """Test print_error when silent and valid"""
    message = "Success: Input is valid"

    with patch("builtins.print") as mock_print:
        print_success(message, is_silent=True)
        mock_print.assert_not_called()


############ print_divider ############
# 2 valid cases


def test_print_divider_when_not_silent_valid():
    """Test print_divider when not silent and valid"""
    color = Fore.WHITE
    expected_output = color + "=" * 40 + Style.RESET_ALL

    with patch("builtins.print") as mock_print:
        print_divider(is_silent=False)
        mock_print.assert_called_once_with(expected_output)


def test_print_divider_when_silent_valid():
    """Test print_divider when silent and valid"""

    with patch("builtins.print") as mock_print:
        print_divider(is_silent=True)
        mock_print.assert_not_called()