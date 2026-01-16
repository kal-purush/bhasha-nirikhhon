import yaml

from argparse import ArgumentParser

parser = ArgumentParser()
parser.add_argument("-c", "--config", dest="config", help="path to config file", metavar="CONF")
parser.add_argument("-p", "--project", dest="project", help="path to config file", metavar="PROJ")
parser.add_argument("-z", "--zone", dest="zone", help="number of workers", metavar="ZONE")
parser.add_argument("-i", "--instance", dest="instance", help="number of workers", metavar="INST")
parser.add_argument("-a", "--active-hours", dest="active_hours", help="number of workers", metavar="ACT")
parser.add_argument("-w", "--week-days", dest="week_days", help="number of workers", metavar="WEEK")

args = parser.parse_args()

with open(args.config) as file:
    config = yaml.full_load(file)

if not config[args.project]:
   config[args.project]