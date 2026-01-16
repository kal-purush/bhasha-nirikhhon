"""new-python-project tests.

© Reuben Thomas <rrt@sc3d.org> 2025

Released under the GPL version 3, or (at your option) any later version.
"""

from pathlib import Path

import pytest
from nancy import main
from pytest import CaptureFixture, LogCaptureFixture
from testutils import failing_cli_test


tests_dir = Path(__file__).parent.resolve() / "test-files"


# CLI tests
def test_help_option_should_produce_output(capsys: CaptureFixture[str]) -> None:
    with pytest.raises(SystemExit) as e:
        main(["--help"])
    assert e.type is SystemExit
    assert e.value.code == 0
    assert capsys.readouterr().out.find("A simple templating system.") != -1


def test_invalid_command_line_argument_causes_an_error(
    capsys: CaptureFixture[str],
    caplog: LogCaptureFixture,
) -> None:
    failing_cli_test(capsys, caplog, ["--foo", "a"], "unrecognized arguments: --foo")