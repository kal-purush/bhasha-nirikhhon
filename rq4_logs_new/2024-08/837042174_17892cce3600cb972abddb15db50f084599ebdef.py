import argparse
from colorama import Fore, Style

# Valid Inputs
# [x] gos -d ../../sampledirectory -y 'text_to_find' -n 'text_to_avoid'
# [x] gos -f ../../samplefile.txt -y 'text_to_find' -n 'text_to_avoid'
# TODO: [ ] gos -d ../../sampledirectory -c ../../config.json
# TODO: [ ] gos -f ../../samplefile.txt -c ../../config.json
# [x] gos -d ../../sampledirectory -y 'text_to_find'
# [x] gos -f ../../samplefile.txt -y 'text_to_find'
# [x] gos -d ../../sampledirectory -n 'text_to_avoid'
# [x] gos -f ../../samplefile.txt -n 'text_to_avoid'

# REFACTOR: [ ] Split logger into its own file inside /utils


# DOCS: Add docsstring
def print_values(message, is_silent=False, color=Fore.WHITE):
    if is_silent:
        return

    # Split the message into parts based on the colon
    if ":" in message:
        left, right = message.split(":", 1)
        left = left.strip()  # Remove any extra spaces around the left part
        right = right.strip()  # Remove any extra spaces around the right part

        # Print with different colors for each part
        print(
            Fore.WHITE
            + left
            + Style.RESET_ALL
            + Fore.YELLOW
            + ": "
            + right
            + Style.RESET_ALL
        )
    else:
        # If no colon is present, just print the message in the specified color
        print(color + message + Style.RESET_ALL)


# DOCS: Add docsstring
def print_info(message, is_silent=False, color=Fore.WHITE):
    if is_silent:
        return
    print(color + message + Style.RESET_ALL)


# DOCS: Add docsstring
def print_divider(is_silent=False, color=Fore.WHITE):
    if is_silent:
        return
    print(color + "=" * 40 + Style.RESET_ALL)


# DOCS: Add docsstring
def print_header(message, is_silent=False, color=Fore.WHITE):
    if is_silent:
        return
    print(color + message + Style.RESET_ALL)


# DOCS: Add docsstring
def print_header_with_divider(message, is_silent=False):
    print("\n")
    print_divider(is_silent)
    print_header(message, is_silent)
    print_divider(is_silent)


# DOCS: Add docsstring
def parse_and_validate_input():
    parser = argparse.ArgumentParser(
        description="With GOS (Grep on Steriods), you can efficiently check whether certain strings exist or do not exist in your files or directories."
    )
    parser.add_argument(
        "-y",
        "--should",
        nargs="?",
        help="String that should exists in the given file/directory",
    )
    parser.add_argument(
        "-n",
        "--should_not",
        nargs="?",
        help="String that should not exists in the given file/directory",
    )
    parser.add_argument("-f", "--file", nargs="?", help="File to search in")
    parser.add_argument("-d", "--directory", nargs="?", help="Directory to search in")
    parser.add_argument(
        "-c",
        "--config",
        nargs="?",
        help='JSON config file specifying "should" and "should-not" strings',
    )
    parser.add_argument(
        "-s",
        "--silent",
        action="store_true",
        help="Suppress output, returns only true/false",
    )
    args = parser.parse_args()

    if not (args.file or args.directory):
        parser.error("Either file (-f) or directory (-d) is required")

    if not (args.should or args.should_not or args.config):
        parser.error(
            "Either provide string to look for in -y (should have), -n (should not have) or -c (config json file) "
        )

    if args.file and args.directory:
        parser.error("Only one of file (-f) or directory (-d) is allowed")

    if (args.should and args.should_not) and args.config:
        parser.error(
            "Only one of -y (should have) or -n (should not have) or -c (config json file) is allowed"
        )
    return args


# DOCS: Add docsstring
def create_flags_from_input(args):

    print_header_with_divider("Configuration Summary:", args.silent)

    # TODO: Check if Config file is a file and not a directory
    # TODO: Check if File is a file and not a directory
    # TODO: Check if Directory is a directory and not a file
    is_file = False
    if args.file:
        print_values(f"Searching for string in file: {args.file}", args.silent)
        is_file = True
    else:
        print_values(
            f"Searching for string in directory: {args.directory}", args.silent
        )

    is_inline = False
    if args.should or args.should_not:
        is_inline = True
    else:
        print_values(f"Using config file: {args.config}", args.silent)

    is_should, is_should_not = False, False
    if args.should:
        print_values(f"Should contain: {args.should}", args.silent)
        is_should = True

    if args.should_not:
        print_values(f"Should not contain: {args.should_not}", args.silent)
        is_should_not = True

    print_header(
        "✅ Configuration details are set. Proceeding with the next steps.",
        args.silent,
    )
    return {
        "is_file": is_file,
        "is_inline": is_inline,
        "is_should": is_should,
        "is_should_not": is_should_not,
    }


# DOCS: Add docsstring
def main():
    args = parse_and_validate_input()
    flags_dict = create_flags_from_input(args)


if __name__ == "__main__":
    main()