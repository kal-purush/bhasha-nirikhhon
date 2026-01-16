"""Meme class file."""

import random
import json
from os import walk

class Meme(object):
    """Class for getting memes."""

    MEMES_F = r"ressource//memes"
    INTERNET_MEME_F = r"ressource//memes//internet.json"

    def __init__(self):
        """."""
        self.file_net = []
        with open(self.INTERNET_MEME_F, 'r') as f:
            self.file_net = json.load(f)
        self.files_server = []
        for (_, _, filenames) in walk(self.MEMES_F):
            self.files_server.extend(filenames)
            break

    def get_meme(self):
        """Return a file or a path to a meme."""
        if len(self.files_server) != 0 and random.randrange(0, 2) == 0:
            return ('file', self.MEMES_F + '//' + self.files_server[random.randrange(0, len(self.files_server))])
        else:
            return ('message', self.file_net[random.randrange(0, len(self.file_net))])