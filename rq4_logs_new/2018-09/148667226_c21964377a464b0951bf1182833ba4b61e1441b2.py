import sys
import typing as t
import json
import os.path
import logging
logger = logging.getLogger(__name__)


def attachment(
    *,
    config_path: str,
    mapping_path: str,
    save: bool = True,
    paths: t.Sequence[str],
    out: t.Optional[t.IO] = None,
) -> None:
    import base64
    from docbasesync import App
    out = out or sys.stdout
    app = App(config_path)
    with app.resource as r:
        contents = []
        for path in paths:
            with open(path, "rb") as rf:
                data = rf.read()
            contents.append(
                {
                    "name": os.path.basename(path),
                    "content": base64.b64encode(data).decode("ascii")
                }
            )
        data = r.attachment(contents)
    json.dump(data, out, indent=2, ensure_ascii=False)


def parse(
    *,
    path: str,
    out: t.Optional[t.IO] = None,
) -> None:
    from docbasesync import parsing
    out = out or sys.stdout
    with open(path) as rf:
        parsed = parsing.parse_article(rf.read())
    parsing.dump(parsed)


def main(argv: t.Optional[t.Sequence[str]] = None) -> None:
    import argparse
    parser = argparse.ArgumentParser(description=None)
    parser.print_usage = parser.print_help
    parser.add_argument('--log', default="INFO", choices=list(logging._nameToLevel.keys()))

    subparsers = parser.add_subparsers(required=True, dest="subcommand")

    # parse
    fn = parse
    sparser = subparsers.add_parser(fn.__name__, description=fn.__doc__)
    sparser.set_defaults(subcommand=fn)
    sparser.add_argument("path")

    # attachment
    fn = attachment
    sparser = subparsers.add_parser(fn.__name__, description=fn.__doc__)
    sparser.set_defaults(subcommand=fn)
    sparser.add_argument('-c', '--config', required=False, dest="config_path")
    sparser.add_argument("--mapping", default=None, type=int, dest="mapping_path")
    sparser.add_argument("--save", action="store_true")
    sparser.add_argument("paths", nargs="+")

    args = parser.parse_args(argv)
    params = vars(args).copy()

    logging.basicConfig(level=getattr(logging, params.pop('log')), stream=sys.stderr)
    params.pop("subcommand")(**params)


if __name__ == '__main__':
    main()