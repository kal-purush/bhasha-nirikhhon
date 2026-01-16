"""Sub command that will generate the content of the static site."""
from autology.utilities import log_file
from autology.configuration import get_configuration
from autology.reports.project import project


def register_command(subparser):
    """Register the subcommand with any additional arguments."""
    update_parser = subparser.add_parser('mkl-project', help='Tools associated with the project report plugin')
    update_parser.set_defaults(func=_main)

    # Arguments
    update_parser.add_argument('-p', '--project-list', help='List all of the project identifiers that have been '
                                                            'defined in this instance', action='store_true')


def _main(args):
    """Determine what method to run based on the arguments provided."""
    if args.project_list:
        _find_all_project_ids()


def _find_all_project_ids():
    """Walk all of the log files, and then retrieve the project definitions."""
    configuration_settings = get_configuration()

    for date_to_process, content_file in log_file.walk_log_files(configuration_settings.processing.inputs):
        project.process_file(content_file, date_to_process)

    print('Defined Projects: ')
    for project_id in sorted(project.get_defined_projects().keys()):
        definition = project.get_defined_projects()[project_id]
        project_name = definition.get('name', '-* UNDEFINED *-')

        print('  {}: {}'.format(project_id, project_name))