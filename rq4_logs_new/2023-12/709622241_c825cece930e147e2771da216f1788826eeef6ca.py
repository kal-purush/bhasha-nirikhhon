from __future__ import annotations

import logging
from collections import Counter

from objectory import OBJECT_TARGET
from pytest import LogCaptureFixture

from flamme.transformer.df import (
    ToNumeric,
    is_dataframe_transformer_config,
    setup_dataframe_transformer,
)

#####################################################
#     Tests for is_dataframe_transformer_config     #
#####################################################


def test_is_dataframe_transformer_config_true() -> None:
    assert is_dataframe_transformer_config(
        {
            OBJECT_TARGET: "flamme.transformer.df.ToNumeric",
            "columns": ["col1", "col3"],
        }
    )


def test_is_dataframe_transformer_config_false() -> None:
    assert not is_dataframe_transformer_config({OBJECT_TARGET: "collections.Counter"})


#################################################
#     Tests for setup_dataframe_transformer     #
#################################################


def test_setup_dataframe_transformer_object() -> None:
    transformer = ToNumeric(columns=["col1", "col3"])
    assert setup_dataframe_transformer(transformer) is transformer


def test_setup_dataframe_transformer_dict() -> None:
    assert isinstance(
        setup_dataframe_transformer(
            {
                OBJECT_TARGET: "flamme.transformer.df.ToNumeric",
                "columns": ["col1", "col3"],
            }
        ),
        ToNumeric,
    )


def test_setup_dataframe_transformer_incorrect_type(caplog: LogCaptureFixture) -> None:
    with caplog.at_level(level=logging.WARNING):
        assert isinstance(
            setup_dataframe_transformer({OBJECT_TARGET: "collections.Counter"}), Counter
        )
        assert caplog.messages