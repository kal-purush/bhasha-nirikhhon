"""Command-line tool to demonstrate layer loading"""

import argparse
import functools
import importlib
import json
from typing import Callable, cast

import layer_loader


def import_dotted(name: str, kind: str) -> Callable:
    module_name, attribute_name = name.rsplit('.', maxsplit=1)
    module = importlib.import_module(module_name)
    func = getattr(module, attribute_name)

    if not callable(func):
        raise argparse.ArgumentError("{} {} must be callable.".format(kind.title(), name))

    return cast(Callable, func)


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        prog='python -m layer_loader',
        description="Command to demonstrate layer loading",
    )
    parser.add_argument(
        '--loader',
        required=True,
        type=functools.partial(import_dotted, kind='loader'),
        help="The loader to use for the given files. Should be specified as a "
             "dotted path to a callable which works on file objects and returns "
             "dict objects, for example 'json.load'.",
    )
    parser.add_argument(
        '--dumper',
        type=functools.partial(import_dotted, kind='dumper'),
        default=functools.partial(json.dumps, sort_keys=True, indent=4),
        help="The dumper to use for the given files. Should be specified as a "
             "dotted path to a callable which works on dict objects and returns "
             "a string, for example 'pprint.pformat' (default: 'json.dumps').",
    )
    parser.add_argument(
        'files',
        metavar='FILES',
        nargs='+',
        type=argparse.FileType('r'),
    )
    return parser.parse_args()


def main(options: argparse.Namespace) -> None:
    data = layer_loader.load_files(
        options.files,
        options.loader,
    )

    print(options.dumper(data))


if __name__ == '__main__':
    main(parse_args())