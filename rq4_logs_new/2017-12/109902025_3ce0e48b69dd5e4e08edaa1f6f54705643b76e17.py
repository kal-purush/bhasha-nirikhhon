"""Sub command that will generate the content of the static site."""
import pathlib

from autology.configuration import get_configuration
from autology.commands.subcommands import updaters


def register_command(subparser):
    """Register the subcommand with any additional arguments."""
    generator_parser = subparser.add_parser('update', help='Run the log files through an updater.  Used to update '
                                                           'between versions of autology')
    generator_parser.set_defaults(func=_main)


def _main(args):
    configuration_settings = get_configuration()

    # Need to find all of the files that are stored in the input_files directories in order to start building the
    # reports that will be used to generate the static log files.
    for input_path in configuration_settings.processing.inputs:
        search_path = pathlib.Path(input_path)

        # Currently going to make the assumption that everyone is using the path naming convention that I'm dictating
        # which is YYYY/MM/DD/file.ext
        for file_component in search_path.glob('*/*/*/*'):
            # Store all of the files into a dictionary containing the keys and a list of the files that are associated
            # with that day
            updaters.update_files(search_path, file_component)