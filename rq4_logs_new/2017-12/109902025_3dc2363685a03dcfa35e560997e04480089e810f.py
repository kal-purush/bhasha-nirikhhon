import requests
import zipfile
import pathlib
import yaml
import io
import re
import shutil
from autology.configuration import get_configuration_root

DEFAULT_TEMPLATES_URL = 'https://github.com/MeerkatLabs/autology_templates/archive/v0.0.1.zip'


def get_template_directory():
    """Provide the path to the template directory based on the main path (where the configuration file is located)."""
    # template output directory is output/templates, so need to create that location before pulling out the templates
    template_location = get_configuration_root() / 'templates' / 'output'
    template_location.mkdir(parents=True, exist_ok=True)
    return template_location


def install_template(templates_directory, template_definition):
    """
    Install the template pointed to by template_definition into the templates directory provided.
    :param templates_directory:
    :param template_definition:
    :return:
    """
    try:
        template_request = requests.get(template_definition)
        template_file = io.BytesIO(template_request.content)
    except requests.exceptions.MissingSchema:
        template_file = template_definition

    try:
        templates_path = _process_zip_file(template_file, templates_directory)
    except IsADirectoryError:
        templates_path = _process_directory(template_file, templates_directory)

    return templates_path


def _process_zip_file(template_file, templates_directory):
    templates_path = None

    # Extract the entire contents of the zip file and store them in the templates/output directory of the project area
    with zipfile.ZipFile(template_file) as template_zip:
        template_definition_file = None

        # Verify that the root directory has a template.yaml file that will contain the configuration details
        for file_name in template_zip.namelist():
            zip_path = pathlib.PurePath(file_name)
            if zip_path.match('template.yaml'):
                template_definition_file = zip_path
                break

        if not template_definition_file:
            return None

        template_definition = yaml.load(template_zip.open(str(template_definition_file)))
        name = template_definition.get('name', 'Autology Template')
        version = template_definition.get('version', '0.0.0')

        dir_name = _create_directory_name(name, version)
        destination_directory = templates_directory / dir_name

        if destination_directory.exists():
            print('Cannot copy templates into place because directory: {} already exists'.format(destination_directory))
            return None

        template_definition_directory = template_definition_file.parent
        for file_name in template_zip.namelist():

            zip_info = template_zip.getinfo(file_name)
            if zip_info.is_dir():
                continue

            zip_path = pathlib.PurePath(file_name)
            try:
                sub_path = zip_path.relative_to(template_definition_directory)
                file_path = destination_directory / sub_path
                file_path.parent.mkdir(parents=True, exist_ok=True)

                shutil.copyfileobj(io.TextIOWrapper(template_zip.open(file_name)), open(str(file_path), 'w'))
            except ValueError:
                # This path isn't relative to the template definition directory so should be ignored.
                pass

    return destination_directory


def _process_directory(template_file, templates_directory):

    print('template_path: {}'.format(template_file))
    template_path = pathlib.Path(template_file)

    template_definition_file = None
    # Need to find the template definition file first.
    for template_definition_file in template_path.glob('**/template.yaml'):
        print('found file: {}'.format(template_definition_file))
        break

    if template_definition_file is None:
        return

    # Convert the name of the template and the version of the template into a directory name
    with template_definition_file.open() as _file_ptr:
        template_definition = yaml.load(_file_ptr)

    name = template_definition.get('name', 'Autology Template')
    version = template_definition.get('version', '0.0.0')

    dir_name = _create_directory_name(name, version)
    destination_directory = templates_directory / dir_name

    try:
        shutil.copytree(template_definition_file.parent, destination_directory)
    except FileExistsError:
        print('Cannot copy templates into place because directory: {} already exists'.format(destination_directory))
        return None

    return destination_directory


def _create_directory_name(name, version):
    unallowed_characters = re.compile(r'[^-a-z0-9]+')

    dir_name = "{}-{}".format(name, version).lower()
    dir_name = re.sub(unallowed_characters, '-', dir_name)

    return dir_name