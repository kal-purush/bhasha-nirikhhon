#Imports
import argparse
import logging, logging.config
import yaml
import io
import sys

#Load base logging information
loggerconfig_stream = open("logger.ini", "r")
log_config = yaml.safe_load(loggerconfig_stream)
logging.config.dictConfig(log_config)

logger = logging.getLogger(__name__)

class CmdInterface:

    def parse_args(self, args):

        parsed_args = self.arg_parser.parse_args(args)

        return parsed_args

    def load_script_settings(self, settings_path):

        with io.open(settings_path, "r") as settings_yml:
            try:
                settings_dict = yaml.safe_load(settings_yml)
                logger.debug("Loaded command interface settings")
            except:
                logger.error("Command interface settings failed to load from file: " + settings_path)

        return settings_dict

    def create_arg_parser(self):

        logger.debug("Creating argument parser")
        parser = argparse.ArgumentParser(prog=self.settings.get("prog"),
                                                  description=self.settings.get("description"),
                                                  epilog=self.settings.get("epilog"))

        argument_settings_list = self.settings.get("arguments")

        for argument in argument_settings_list:
            print(argument)
            parser.add_argument(argument.get("name"), dest=argument.get("dest"))


        return parser

    def run(self, args):

        parsed_args = self.parse_args(args)

        print(parsed_args)
        pass

    def __init__(self, func, settings_path):

        logger.debug("Initializing command line interface class")
        self.func = func
        self.settings = self.load_script_settings(settings_path)

        self.arg_parser = self.create_arg_parser()
        pass

def forge():
    pass

c = CmdInterface(forge,"./settings.yml")

if __name__ == "__main__":
    c.run(sys.argv[1:])