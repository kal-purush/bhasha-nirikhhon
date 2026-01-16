import sys
import typing as t
import json
import logging
logger = logging.getLogger(__name__)


def search(
    *,
    config: str,
    query: t.Optional[str] = None,
    page: t.Optional[int] = None,
    per_page: t.Optional[int] = None,
    out: t.Optional[t.IO] = None,
) -> None:
    from docbasesync import App
    out = out or sys.stdout
    app = App(config)
    with app:
        data = app.search(q=query, page=page, per_page=per_page)
    json.dump(data, out, indent=2, ensure_ascii=False)


def main(argv: t.Optional[t.Sequence[str]] = None) -> None:
    import argparse
    parser = argparse.ArgumentParser(description=None)
    parser.print_usage = parser.print_help
    parser.add_argument('--config', required=False)
    parser.add_argument('--log', default="INFO", choices=list(logging._nameToLevel.keys()))

    subparsers = parser.add_subparsers(required=True, dest="subcommand")

    # search
    fn = search
    sparser = subparsers.add_parser(fn.__name__, description=fn.__doc__)
    sparser.set_defaults(subcommand=fn)
    sparser.add_argument("-q", "--query", default=None)
    sparser.add_argument("--page", default=None, type=int)
    sparser.add_argument("--per_page", default=None, type=int)

    args = parser.parse_args(argv)
    params = vars(args).copy()

    logging.basicConfig(level=getattr(logging, params.pop('log')), stream=sys.stderr)
    params.pop("subcommand")(**params)


if __name__ == '__main__':
    main()