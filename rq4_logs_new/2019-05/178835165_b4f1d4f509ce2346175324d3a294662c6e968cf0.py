import importlib
import pytest
from test_utils.test_utils import serialise_mocks
ssl_enum_ciphers = importlib.import_module("results_parser.ssl-enum-ciphers")


@pytest.mark.unit
@serialise_mocks()
def test_adds_to_empty():
    summaries = {}
    ssl_enum_ciphers.summarise_proto("TLSv1.1", summaries)
    assert summaries["lowest_ssl_proto"] == "TLSv1.1"
    assert "unknown_ssl_proto" not in summaries


@pytest.mark.unit
@serialise_mocks()
def test_adds_unknown_flag():
    summaries = {}
    ssl_enum_ciphers.summarise_proto("nonsenes", summaries)
    ssl_enum_ciphers.summarise_proto("TLSv1.1", summaries)
    assert summaries["lowest_ssl_proto"] == "TLSv1.1"
    assert summaries["unknown_ssl_proto"]


@pytest.mark.unit
@serialise_mocks()
def test_orders_by_protocol():
    summaries = {
        "lowest_ssl_proto": "TLSv1.1"
    }
    ssl_enum_ciphers.summarise_proto("TLSv1.2", summaries)
    ssl_enum_ciphers.summarise_proto("TLSv1.0", summaries)
    ssl_enum_ciphers.summarise_proto("silly", summaries)
    assert summaries["lowest_ssl_proto"] == "TLSv1.0"
    assert summaries["unknown_ssl_proto"]
