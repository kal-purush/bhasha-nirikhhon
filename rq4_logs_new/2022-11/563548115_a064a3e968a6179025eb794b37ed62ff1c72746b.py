import os
import json


class PieceNotFoundException(Exception):
    """Raised when a piece is not found in the database"""

    pass


class MusicController:
    def __init__(self):
        self.music_path = os.path.relpath("data/music.csv")
        # open file
        try:
            with open("data/music.json", "r") as music_file:
                self.music = json.load(music_file)
        # if file doesn't exist, create it, write headers and open it
        except FileNotFoundError:
            filename = "data/music.json"
            with open(filename, "w") as music_file:
                json.dump([], music_file)
                music_file.close()
            with open("data/music.json") as music_file:
                self.music = json.load(music_file)

    def add_music(self, music):
        """
        Adds music to the music file

        Is called by the Populator class when the user wants to add new music to the file via the command line.

        Parameters
        ----------
        music : Dict
            Dictionary of the music to be added to the file
        """
        self.music.append(music)
        with open("data/music.json", "w") as music_file:
            json.dump(self.music, music_file)

    def get_music(self, id):
        """
        Returns the music with the given id

        Parameters
        ----------
        id : int
            The id of the music to be returned

        Returns
        -------
        Dict
            The music with the given id
        """
        for piece in self.music:
            if piece["id"] == id:
                return piece
        raise PieceNotFoundException("Piece with id {} not found".format(id))


if __name__ == "__main__":
    c = MusicController()
    print(c.music)