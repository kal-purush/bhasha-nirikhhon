import functools
import pathlib
import subprocess

from . import charm

_REPOSITORY_DIRECTORY = pathlib.Path("charm-repo")
_run = functools.partial(
    subprocess.run, cwd=_REPOSITORY_DIRECTORY, check=True, capture_output=True, text=True
)


class CharmNotFound(Exception):
    """Charm repository or ref not found"""


def checkout(charm_ref: charm.CharmRef, *, sparse=True):
    """Checkout charm

    Returns path to charm directory

    Raises `CharmNotFound`
    """
    _REPOSITORY_DIRECTORY.mkdir()
    _run(["git", "init"])
    if sparse:
        _run(
            [
                "git",
                "sparse-checkout",
                "set",
                "--sparse-index",
                charm_ref.relative_path_to_charmcraft_yaml,
            ]
        )
    try:
        _run(
            [
                "git",
                "remote",
                "add",
                "--fetch",
                "origin",
                f"https://github.com/{charm_ref.github_repository}.git",
            ]
        )
    except subprocess.CalledProcessError as exception:
        if "ERROR: Repository not found." in exception.stderr:
            raise CharmNotFound(f"{charm_ref.github_repository=} not found")
        else:
            raise
    try:
        _run(["git", "fetch", "origin", charm_ref.ref])
    except subprocess.CalledProcessError as exception:
        if "couldn't find remote ref" in exception.stderr:
            raise CharmNotFound(f"{charm_ref.ref=} not found")
        else:
            raise
    _run(["git", "checkout", "FETCH_HEAD"])
    path = _REPOSITORY_DIRECTORY / charm_ref.relative_path_to_charmcraft_yaml
    assert path.resolve().is_relative_to(_REPOSITORY_DIRECTORY.resolve())
    print(f"[ccc-hub] Checked out {charm_ref=}", flush=True)
    return path