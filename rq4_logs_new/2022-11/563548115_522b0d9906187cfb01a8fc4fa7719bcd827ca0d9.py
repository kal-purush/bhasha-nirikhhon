import argparse

from controller.Controller import MusicController


def main():
    parser = argparse.ArgumentParser(
        prog="Pyductor",
        description="Generating music to help you choose what to play",
        epilog="Enjoy your music!",
    )
    parser.add_argument(
        "-c",
        "--command",
        required=False,
        help="The command to be executed, choices are: add, get, update, delete, list",
        metavar="command",
        type=str,
        choices=["add", "get", "update", "delete", "list", "recommend"],
    )
    args = parser.parse_args()

    cmd = args.command

    controller = MusicController()

    if cmd == "add":
        controller.add_music()
    elif cmd == "get":
        controller.get_music()
    elif cmd == "update":
        controller.update_music()
    elif cmd == "delete":
        controller.delete_music()
    elif cmd == "list":
        controller.list_music()
    elif cmd == "recommend":
        controller.recommend_music()
    else:
        print("Please enter a valid command")


if __name__ == "__main__":
    main()