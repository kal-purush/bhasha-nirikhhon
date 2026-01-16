import click
import pytest
from click.testing import CliRunner

import clicktypes


def cli(validator: str, **kwargs):
    @click.command(help=validator)
    @click.argument(
        "value",
        type=getattr(clicktypes, validator)(**kwargs),
    )
    def main(value):
        click.echo(f"valid {value=}")

    return main


@pytest.mark.parametrize("validator", clicktypes.__all__)
def test_help(validator):
    runner = CliRunner()
    result = runner.invoke(cli(validator), ["--help"])
    assert result.exit_code == 0
    assert validator in result.output


@pytest.mark.parametrize(
    "validator",
    clicktypes.__all__,
)
def test_none(validator):
    runner = CliRunner()
    result = runner.invoke(cli(validator), [""])
    assert result.exit_code == 2


@pytest.mark.parametrize(
    "validator,value",
    [
        ("email", "fu@bar.com"),
    ],
)
def test_cli(validator, value):
    runner = CliRunner()
    result = runner.invoke(cli(validator), [value])
    assert result.exit_code == 0