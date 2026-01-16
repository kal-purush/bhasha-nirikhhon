"""!age Gets the age of the current deployment"""
import os
from os.path import dirname
import re
import time


def age(eq):
    parent_path = dirname(dirname("/"))
    return "Ragebot was last modified: %s" % time.ctime(os.path.getmtime(parent_path))


def on_message(msg, server):
    text = msg.get("text", "")
    match = re.findall(r"!age (.*)", text)
    if not match: return

    return age(match[0])


if __name__ == "__main__":
    age("")