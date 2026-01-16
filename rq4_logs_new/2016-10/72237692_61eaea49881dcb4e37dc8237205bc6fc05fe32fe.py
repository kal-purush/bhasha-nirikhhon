#!/usr/bin/python

import sys
import os
import argparse
import errno
from jinja2 import Environment, FileSystemLoader

def ensure_dir(dirname):
    """
    Ensure that a named directory exists; if it does not, attempt to create it.
    """
    try:
        os.makedirs(dirname)
    except OSError, e:
        if e.errno != errno.EEXIST:
            raise


parser = argparse.ArgumentParser(description='Create ansible roles basic structure folder.')
#parser.add_argument("-v", "--verbose", help="increase output verbosity", action="store_true")
parser.add_argument("-p", "--path", help="Path were the role is generated")
parser.add_argument("role", help="Name of the ansible role")

args = parser.parse_args()

#Create folder structure
folders_to_create = ["files","templates","tasks","handlers","vars","defaults","meta"]
if args.path is None:
    args.path = os.getcwd()  #current dir

for directory in folders_to_create:
    if not os.path.exists("{0}/{1}/{2}".format(args.path,args.role,directory)):
        os.makedirs("{0}/{1}/{2}".format(args.path,args.role,directory))

#Define basic
templates = ["defaults.j2","handlers.j2","tasks.j2","vars.j2"]
env = Environment(loader=FileSystemLoader('templates'))


output = env.get_template('README.md.j2').render()
with open("{0}/README.md".format(args.role), "wb") as fh:
    fh.write(output)

for template in templates:
    output = env.get_template(template).render()
    # to save the results
    with open("{0}/{1}/main.yaml".format(args.role,template[:-3]), "wb") as fh:
        fh.write(output)