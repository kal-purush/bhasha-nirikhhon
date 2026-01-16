import pytest


testinfra_hosts = ["alpine38"]


@pytest.mark.parametrize('pkg', [
  'tar',
  'unzip'
])
def test_pkg(host, pkg):
    package = host.package(pkg)
    assert package.is_installed