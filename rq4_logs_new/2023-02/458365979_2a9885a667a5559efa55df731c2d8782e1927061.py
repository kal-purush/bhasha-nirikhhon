import os
from time import sleep
import testinfra.utils.ansible_runner

testinfra_hosts = testinfra.utils.ansible_runner.AnsibleRunner(
    os.environ['MOLECULE_INVENTORY_FILE']).get_hosts('all')

def test_package(host):
    coturn = host.package("coturn")
    assert coturn.is_installed

def test_service(host):
    coturn = host.service("coturn")
    assert coturn.is_running
    assert coturn.is_enabled

def test_ports(host):
    assert host.socket("udp://127.0.0.1:3478").is_listening
    assert host.socket("udp://127.0.0.1:3479").is_listening

