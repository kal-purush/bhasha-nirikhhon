#! /usr/bin/env python3


from argparse import ArgumentParser, FileType
from json import load
from os import remove
from os.path import isdir, islink, join
from sys import exit


def main():
    parser = ArgumentParser(
        prog="Link Uninstaller",
        description="""Remove symbolic links (ln -s) for a set of files using the target location.""",
        epilog="""The format for the JSON file is a list of a list of pairs.
  [["target-link-name", "source-file-name"],...]""",
    )
    parser.add_argument("target", help="The directory of the target links.")
    parser.add_argument(
        "files",
        type=FileType(mode="r"),
        help="JSON file containing a list of files to be linked.",
    )
    args = parser.parse_args()

    #
    # Setup
    #
    target_dir = args.target
    link_list = load(args.files)
    args.files.close()

    #
    # Check source & target dirs
    #
    if not isdir(target_dir):
        exit("{} is not a directory.".format(target_dir))

    #
    # Start uninstall
    #
    if len(link_list) == 0:
        exit("Oops: no links to remove.")

    for fp in link_list:
        if len(fp) != 2:
            print(
                "Skipping {}. Expected list like ['source-file', 'target-file'].".format(
                    str(fp)
                )
            )
            continue

        target_f = join(target_dir, fp[1])
        if islink(target_f):
            remove(target_f)
            print("Symlink " + target_f + " removed.")


if __name__ == "__main__":
    main()
else:
    print("** Did not plan on being imported **")