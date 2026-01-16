"""Define necessary setup fixtures."""
import pytest
import pkg_resources
from productionsystem.config import ConfigSystem


@pytest.fixture(scope="session", autouse=True)
def config():
    """Set up the config entrypoint map."""
    config_instance = ConfigSystem.setup(None)  # pylint: disable=no-member
    config_instance.entry_point_map = pkg_resources.get_entry_map('productionsystem')
    return config_instance