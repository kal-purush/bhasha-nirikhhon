import argparse
import os

import pytoml
import spittalpond


def verify_config(config):
    """ Verifies the config dictionary-like object for correctness.

    Args:
        config (dict): A python dictionary containing the config data.

    Returns:
        bool: True if the config passes all "tests".
              If config does not pass all "tests" an exception will be raised.
    """

    def quick_msg(msg):
        return ' '.join([prefix, msg, suffix])

    if 'meta' in config.keys():
        prefix = "FATAL:"
        suffix = "not specified in the meta section!"
        if 'url' not in config['meta'].keys():
            raise Exception(quick_msg("URL"))

    if 'login' not in config.keys():
        prefix = "FATAL:"
        suffix = "not specified in the login section!"
        if 'user' not in config['login'].keys():
            raise Exception(quick_msg("user"))
        elif 'password' not in config['login'].keys():
            raise Exception(quick_msg("password"))

    if 'model' in config.keys():
        prefix = "FATAL:"
        suffix = "not specified in the model section!"
        if 'upload' in config['model'].keys():
            if 'do_timestamps' not in config['model']['upload'].keys():
                config['model']['upload']['do_timestamps'] = True
            if 'directory_path' not in config['model']['upload'].keys():
                raise Exception(quick_msg('directory_path'))
            profiles = {k: v for k, v in config['model']\
                        if k in ['dict', 'version']}
            for profile in profiles:
                if 'filename' not in profile.keys():
                    raise Exception(quick_msg('filename'))
                if 'module_supplier_id' not in profile.keys():
                    raise Exception(quick_msg('module_supplier_id'))

    return True

def prepare_files_in_section(spittal_sub_instance, section):
    """ Prepares all specified files in a section.

    Args:
        spittal_sub_instance (SpittalBase):
            Any object that inherits from SpittalBase.
        section (dict):
            Dict epresentation of model or exposure config section.
    """

    # With s being config section and p being Oasis profile type.
    for skey, sitems in section.iteritems():
        if skey in ['dict', 'version']:
            for pkey, pitems in sitems.iteritems():
                data_name = skey + "_" + pkey
                filepath = os.path.join(
                    section['directory_path'],
                    pitems['filename']
                )
                spittal_sub_instance.prepare_file(
                    data_name,
                    filepath,
                    pitems['module_supplier_id'],
                    section['do_timestamps']
                )


def run_model(spittal_instance, config):
    """ Run model section of the config.

    This method assumes that the config is already validated.

    Args:
        spittal_instance (SpittalPond): SpittalPond object to run with.
        config (dict): Contains specific details for running the model.
    """

    spit = spittal_instance

    prepare_files_in_section(spit.model, config['model'])


def runner(config_file):
    """ Parse config file and makes the appropriate Oasis API call as needed."""

    # Load the config file into a toml object.
    with open(args.config_file, 'rb') as f:
        config = pytoml.load(f)

    verify_config(config)

    # If the config verifies we can assume that it has a legit login section.
    spit = spittalpond.SpittalPond(
        base_url=config['meta']['url'],
        pub_user=config['login']['user'],
        log_file=config['meta']['log_file']
    )

    spit.model.do_login(config['login']['password'])

    if 'model' in config.keys():
        run_model(spit, config)

if __name__ == "__main__":
    # Grab the first argument passed. This is the file name.
    parser = argparse.ArgumentParser(
        description="Makes Oasis API calls according to the toml config file \
                     specified."
        )
    parser.add_argument(
        "config_file",
        metavar="file",
        type=str,
        help="an integer for the accumulator"
    )
    args = parser.parse_args()

    runner(args.config_file)