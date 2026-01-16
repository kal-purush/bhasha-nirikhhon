import os
import sys

parent_dir = os.path.abspath(os.path.join(os.path.dirname(__file__), os.pardir))
sys.path.append(parent_dir)


builds = []
roles = ["Main-DPS", "Sub-DPS", "Support"]
build = {
    "role": None,
    "weapon": None,
    "replacements": None,
    "artifacts": None,
    "main_stats": {"sands": None, "goblet": None, "circlet": None},
    "sub_stats": [],
    "rating": None,
}


def character_builds():
    pass


if __name__ == "__main__":
    character_builds()