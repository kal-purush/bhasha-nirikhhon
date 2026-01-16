import csv
import sys
from pathlib import Path
from subprocess import CalledProcessError
from typing import List

import click
from jinja2 import Template
from vagrant import Vagrant


_DISTRO_CONFIG_FILE = "nixtrobed.distros"
_VAGRANTFILE_TEMPLATE_PATH = "Vagrantfile.jinja"
_PLAYBOOK_DIRECTORY = "provisioning/playbooks"
_DEFAULT_PLAYBOOK_NAME = "default.yml"


CONTEXT_SETTINGS = {"help_option_names": ["-h", "--help"]}


@click.group(context_settings=CONTEXT_SETTINGS)
def main() -> int:
    return 0


@main.command(name="init")
@click.argument("directory", type=click.Path())
def initialize_testbed_directory(directory):
    """Generate and initialize a new testbed directory.

    \b
    DIRECTORY is the target directory. This must not exist yet."""
    directory = Path(directory)
    _create_directory_structure(directory)
    _write_default_config_file(directory)
    _write_vagrantfile_template(directory)
    _generate_default_playbook(directory)
    return 0


@main.command(name="start")
@click.argument("distros", type=str, nargs=-1)
def start_distro_boxes(distros: tuple) -> int:
    """Start the boxes of the given distributions.

    This is basically just a wrapper for "vagrant up".

    \b
    DISTRO one or more names of distributions to start."""
    _assert_cwd_is_nixtrobed_directory()
    _generate_vagrantfile(_parse_distro_config())
    vagrant = Vagrant(quiet_stdout=False, quiet_stderr=False)

    if len(distros) < 1 or "all" in distros:
        vagrant.up()
    else:
        for name in distros:
            try:
                vagrant.up(vm_name=name)
            except CalledProcessError as err:
                print(
                    "An error occurred when calling Vagrant. See above for details.",
                    file=sys.stderr,
                )
                return err.returncode
    return 0


@main.command(name="stop")
@click.argument("distros", type=str, nargs=-1)
def stop_distro_boxes(distros: tuple) -> int:
    """Stop the boxes of the given distributions.

    This is basically just a wrapper for "vagrant halt".

    \b
    DISTRO one or more names of distributions to start."""
    _assert_cwd_is_nixtrobed_directory()
    vagrant = Vagrant(quiet_stdout=False, quiet_stderr=False)
    if len(distros) < 1 or "all" in distros:
        vagrant.halt()
    else:
        for name in distros:
            try:
                vagrant.halt(vm_name=name)
            except CalledProcessError as err:
                print(
                    "An error occurred when calling Vagrant. See above for details.",
                    file=sys.stderr,
                )
                return err.returncode
    return 0


@main.command(name="provision")
@click.argument("distros", type=str, nargs=-1)
def provision_distro_boxes(distros: tuple) -> int:
    """Provision the boxes of the given distributions.

    \b
    DISTRO one or more names of distributions to start."""
    _assert_cwd_is_nixtrobed_directory()
    _generate_vagrantfile(_parse_distro_config())
    vagrant = Vagrant(quiet_stdout=False, quiet_stderr=False)

    if len(distros) < 1 or "all" in distros:
        vagrant.provision()
    else:
        for name in distros:
            try:
                vagrant.provision(vm_name=name)
            except CalledProcessError as err:
                print(
                    "An error occurred when calling Vagrant. See above for details.",
                    file=sys.stderr,
                )
                return err.returncode
    return 0


def _assert_cwd_is_nixtrobed_directory() -> None:
    for required_file in (
        _VAGRANTFILE_TEMPLATE_PATH,
        _DISTRO_CONFIG_FILE,
        "provisioning",
    ):
        if not (Path() / required_file).exists():
            raise RuntimeError(
                f"{required_file} is missing. Are you in a nixtrobed directory?"
            )


def _create_directory_structure(path: Path) -> None:
    path.mkdir()
    Path(path / "provisioning" / "playbooks").mkdir(parents=True)
    Path(path / "provisioning" / "roles").mkdir()


def _write_default_config_file(path: Path) -> None:
    config_path = path / _DISTRO_CONFIG_FILE
    with config_path.open("w") as config:
        writer = csv.DictWriter(config, fieldnames=("name", "box", "playbook"))
        config.write("#name,box,playbook\n")
        for distro in _DEFAULT_DISTRIBUTIONS:
            writer.writerow(distro)


def _write_vagrantfile_template(path: Path) -> None:
    vagrantfile = path / _VAGRANTFILE_TEMPLATE_PATH
    with vagrantfile.open("w") as vfile:
        vfile.write(_VAGRANTFILE_TEMPLATE)


def _generate_default_playbook(path: Path) -> None:
    playbook = Path(path) / _PLAYBOOK_DIRECTORY / _DEFAULT_PLAYBOOK_NAME
    with playbook.open("w") as pbook:
        pbook.write("---\n\n")
        pbook.write("- hosts: all\n")
        pbook.write("  tasks:\n")
        pbook.write("  roles:\n")


def _parse_distro_config() -> List[dict]:
    distro_config = Path() / _DISTRO_CONFIG_FILE
    distros = []
    with distro_config.open("r") as config:
        reader = csv.DictReader(config, fieldnames=("name", "box", "playbook"))
        for record in reader:
            if record["name"][0] == "#":
                continue
            if record["name"] is None or record["name"].strip() == "":
                raise RuntimeError(
                    f"Malformed line in {distro_config}. Line {reader.line_num} contains no distro name."
                )
            if record["box"] is None or record["box"].strip() == "":
                raise RuntimeError(
                    f"Malformed line in {distro_config}. Line {reader.line_num} contains no box name."
                )
            if record["playbook"] is None or record["playbook"].strip() == "":
                record["playbook"] = _DEFAULT_PLAYBOOK_NAME
            distros.append(record)
    return distros


def _generate_vagrantfile(distros) -> None:
    vagrantfile_tpl = Path() / _VAGRANTFILE_TEMPLATE_PATH
    vagrantfile = Path() / "Vagrantfile"
    template = Template(vagrantfile_tpl.open().read())
    template_variables = {
        "distros": distros,
        "autogenerated_warning_message": _AUTOGENERATED_WARNING_MESSAGE,
    }
    with vagrantfile.open("w") as vfile:
        vfile.write(template.render(template_variables))


_DEFAULT_DISTRIBUTIONS = (
    {"name": "alma8", "box": "almalinux/8", "playbook": _DEFAULT_PLAYBOOK_NAME},
    {"name": "fedora33", "box": "generic/fedora33", "playbook": _DEFAULT_PLAYBOOK_NAME},
    {
        "name": "debian11",
        "box": "debian/bullseye64",
        "playbook": _DEFAULT_PLAYBOOK_NAME,
    },
    {"name": "debian10", "box": "debian/buster64", "playbook": _DEFAULT_PLAYBOOK_NAME},
    {"name": "ubuntu2204", "box": "ubuntu/jammy64", "playbook": _DEFAULT_PLAYBOOK_NAME},
    {"name": "ubuntu2004", "box": "ubuntu/focal64", "playbook": _DEFAULT_PLAYBOOK_NAME},
)


_VAGRANTFILE_TEMPLATE = """# -*- mode: ruby -*-
# vi: set ft=ruby :
{{ autogenerated_warning_message }}

Vagrant.configure("2") do |config|
  {% for distro in distros %}
  config.vm.define "{{ distro.name }}" do |{{ distro.name }}|
    {{ distro.name }}.vm.box = "{{ distro.box }}"

    {{ distro.name }}.vm.provision "ansible" do |ansible|
      ansible.playbook = "provisioning/playbooks/{{ distro.playbook }}"
    end
  end
  {% endfor %}
end
"""


_AUTOGENERATED_WARNING_MESSAGE = """#
# WARNING: This file will be overwritten on each invocation of nixtrobed!
#          Make any changes in the Vagrantfile.jinja file.
#
"""


if __name__ == "__main__":
    sys.exit(main())