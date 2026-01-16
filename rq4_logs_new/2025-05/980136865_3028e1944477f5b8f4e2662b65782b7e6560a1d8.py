
# Test suite for Bito Repository #218

import pytest
from src.main import process_data, display_results

def test_process_data():
    result = process_data()
    assert isinstance(result, dict)
    assert result["repo_number"] == 218
    assert "status" in result
    assert "features" in result

def test_display_results(capsys):
    test_data = {"test": "data"}
    display_results(test_data)
    captured = capsys.readouterr()
    assert "Repository Data" in captured.out