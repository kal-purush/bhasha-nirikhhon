import pytest
from controller.Controller import MusicController, PieceNotFoundException


class TestController:
    pop = MusicController()

    def test_populator_build(self):
        assert self.pop.music_path == "data/music.csv", "music_path is not correct"
        assert self.pop.music is not None, "No music file"

    def test_add_music(self):
        assert (
            self.pop.add_music(
                {
                    "id": 1,
                    "name": "test",
                    "composer": "test",
                    "genre": "test",
                    "year": 2020,
                    "duration": 1,
                    "path": "test",
                }
            )
            is None
        ), "add_music() did not return None"

    def get_music(self):
        assert self.pop.get_music(1) is not None, "get_music() did not return music"
        with pytest.raises(PieceNotFoundException):
            self.pop.get_music("A")