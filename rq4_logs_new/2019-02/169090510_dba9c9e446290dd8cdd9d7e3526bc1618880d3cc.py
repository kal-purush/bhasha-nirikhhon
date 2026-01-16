#!/usr/bin/env python

import argparse
import atexit
import logging
import re
import sys
from datetime import datetime
from functools import partial
from pprint import pformat
from time import time
import docker

DEFAULT_AGE = 24 * 60
HELP_DEFAULT_AGE = (
    'How old should be a container to be removed. '
    'Defaults to %d minutes') % DEFAULT_AGE
HELP_NOOP = 'Do nothing'
HELP_VERBOSE = 'Print containers to delete'
HELP_REGEX = 'Container name regex'


def _exit():
    logging.shutdown()


def is_debug_on():
    return logging.getLogger().getEffectiveLevel() == logging.DEBUG


def debug_var(debug, name, var):
    if debug:
        logging.debug('Var %s has: %s' % (name, pformat(var)))


def setup_parser(parser):
    parser.add_argument('--debug', help='debug mode', action='store_true')
    parser.add_argument(
        '--minutes',
        help=HELP_DEFAULT_AGE,
        default=DEFAULT_AGE,
        type=int)
    parser.add_argument('--noop', help=HELP_NOOP, action='store_true')
    parser.add_argument('--verbose', help=HELP_VERBOSE, action='store_true')
    parser.add_argument('regex', help=HELP_REGEX)
    return parser


def validate_args(args):
    checks = [
        (lambda args: args.minutes < 0,
         'minutes should be 0 or bigger\n')]
    if [sys.stderr.write(msg) for checker, msg in checks if checker(args)]:
        sys.exit(1)


def get_docker_client():
    return docker.from_env()


def get_all_containers(client):
    return client.containers.list(all=True, sparse=True)


def get_containers_dicts(containers):
    return [container.attrs for container in containers]


def is_regex_match(pattern, names):
    return any((pattern.search(name) for name in names))


def filter_containers_by_regex(regex, containers):
    pattern = re.compile(regex)
    return [container for container
            in containers
            if is_regex_match(pattern, container.attrs[u'Names'])]


def remove_keys_from_dict(keys, dict_):
    return {k: v for k, v in dict_.items() if k not in keys}


def beautify_container(container):
    new_container = remove_keys_from_dict(
        [u'Command',
         u'HostConfig',
         u'Id',
         u'ImageID',
         u'Labels',
         u'Mounts',
         u'NetworkSettings',
         u'Ports'
         ],
        container)
    new_container[u'Created'] = get_datetime(container[u'Created'])
    return new_container


def print_containers_to_delete(containers):
    print('Containers to delete')
    print(pformat([beautify_container(container)
                   for container in get_containers_dicts(containers)]))


def delete_container(verbose, container):
    try:
        if verbose:
            print("Removing {}".format(container.attrs[u'Names']))
        container.remove(force=True)
    except Exception as e:
        if verbose:
            print(e)


def delete_containers(verbose, containers):
    [delete_container(verbose, container) for container in containers]


def get_containers_to_delete(current_time, minutes, containers):
    return [container
            for container in containers
            if ((current_time - container.attrs[u'Created']) / 60) > minutes]


def get_datetime(timestamp):
    return datetime.fromtimestamp(timestamp).isoformat(' ')


def main():
    atexit.register(_exit)
    parser = setup_parser(argparse.ArgumentParser(
        description='Kill rogue docker containers'))
    args = parser.parse_args()
    if args.debug:
        logging.basicConfig(level=logging.DEBUG)
    debug = partial(debug_var, debug=is_debug_on())
    debug(name='args', var=args)
    validate_args(args)
    client = get_docker_client()
    containers = get_all_containers(client)
    debug(name='containers', var=get_containers_dicts(containers))
    filtered_containers = filter_containers_by_regex(args.regex, containers)
    debug(name='filtered_containers',
          var=get_containers_dicts(filtered_containers))
    current_time = time()
    debug(name='current_time', var=get_datetime(current_time))
    containers_to_delete = get_containers_to_delete(current_time,
                                                    args.minutes,
                                                    filtered_containers)
    debug(name='containers_to_delete',
          var=get_containers_dicts(containers_to_delete))
    if args.verbose:
        print_containers_to_delete(containers_to_delete)
    if args.noop:
        sys.exit(0)
    delete_containers(args.verbose, containers_to_delete)


if __name__ == '__main__':
    main()