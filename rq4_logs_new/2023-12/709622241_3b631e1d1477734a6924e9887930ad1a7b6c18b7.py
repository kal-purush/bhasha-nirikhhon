from __future__ import annotations

import logging
from collections import Counter

from objectory import OBJECT_TARGET
from pytest import LogCaptureFixture

from flamme.transformer.series import (
    ToNumeric,
    is_series_transformer_config,
    setup_series_transformer,
)

##################################################
#     Tests for is_series_transformer_config     #
##################################################


def test_is_series_transformer_config_true() -> None:
    assert is_series_transformer_config({OBJECT_TARGET: "flamme.transformer.series.ToNumeric"})


def test_is_series_transformer_config_false() -> None:
    assert not is_series_transformer_config({OBJECT_TARGET: "collections.Counter"})


##############################################
#     Tests for setup_series_transformer     #
##############################################


def test_setup_series_transformer_object() -> None:
    transformer = ToNumeric()
    assert setup_series_transformer(transformer) is transformer


def test_setup_series_transformer_dict() -> None:
    assert isinstance(
        setup_series_transformer({OBJECT_TARGET: "flamme.transformer.series.ToNumeric"}),
        ToNumeric,
    )


def test_setup_series_transformer_incorrect_type(caplog: LogCaptureFixture) -> None:
    with caplog.at_level(level=logging.WARNING):
        assert isinstance(setup_series_transformer({OBJECT_TARGET: "collections.Counter"}), Counter)
        assert caplog.messages