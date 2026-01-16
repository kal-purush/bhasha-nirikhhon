import click
import pytest
from click.testing import CliRunner

import clicktypes


@click.command(name="email", help="email validator")
@click.option(
    "--email",
    type=clicktypes.email(),
    envvar="EMAIL",
    multiple=True,
)
def cli(email):
    click.echo(f"valid {email=}")


@pytest.mark.parametrize(
    "emails,exit_code",
    [
        ("foo@bar.com", 0),
        ("foo.bar.com", 2),
        ("foo@bar.com bar@foo.com", 0),
        ("foo@bar.com bar.foo.com", 2),
    ],
)
def test_envvar(emails, exit_code):

    runner = CliRunner()
    result = runner.invoke(cli, env={"EMAIL": emails})
    assert result.exit_code == exit_code


if __name__ == "__main__":
    cli()