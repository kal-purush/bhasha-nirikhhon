import pytest

import contracts
from contracts import BackendContractsPlugin, UnknownClassException


def test_multiple_paths_to_change(monkeypatch):
    monkeypatch.setattr(contracts, "DATA_FILE", "tests/data.json")

    markdown = """
# this is a page title

!!! dummy_module.DummyClass

!!! dummy_module2.DummyClass2
    """

    output = BackendContractsPlugin().on_page_markdown(markdown, None, None, None)

    expected = """
# this is a page title


# DummyClass

Dummy docstring







# DummyClass2

Dummy docstring2





    """

    assert output == expected


def test_no_paths_to_change(monkeypatch):
    monkeypatch.setattr(contracts, "DATA_FILE", "tests/data.json")

    output = BackendContractsPlugin().on_page_markdown("", None, None, None)

    assert output == ""


def test_one_path_to_change(monkeypatch):
    monkeypatch.setattr(contracts, "DATA_FILE", "tests/data.json")

    markdown = """
# this is a page title

!!! dummy_module.DummyClass
    """

    output = BackendContractsPlugin().on_page_markdown(markdown, None, None, None)

    expected = """
# this is a page title


# DummyClass

Dummy docstring





    """

    assert output == expected


def test_no_data_file(monkeypatch):
    monkeypatch.setattr(contracts, "DATA_FILE", "tests/non-existent-data.json")

    with pytest.raises(FileNotFoundError):
        BackendContractsPlugin().on_page_markdown("", None, None, None)


def test_unknown_class(monkeypatch):
    monkeypatch.setattr(contracts, "DATA_FILE", "tests/data.json")

    markdown = """
    # this is a page title

    !!! unknown_module.UnknownClass
    """

    with pytest.raises(UnknownClassException):
        BackendContractsPlugin().on_page_markdown(markdown, None, None, None)