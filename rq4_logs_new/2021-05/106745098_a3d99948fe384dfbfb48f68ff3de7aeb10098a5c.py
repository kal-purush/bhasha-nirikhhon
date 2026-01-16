import argparse


def load_args():
    parser = argparse.ArgumentParser(allow_abbrev=False)
    args = parser.parse_args()
    return args


def main():
    args = load_args()


if __name__ == "__main__":
    main()