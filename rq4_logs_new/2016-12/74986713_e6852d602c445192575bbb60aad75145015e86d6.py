from mutagen import File
from mutagen.mp3 import MP3
import glob
from playlist import Playlist, Song


class MusicCrawler:

    def __init__(self, directory):
        self.directory = directory

    def parse_length(self, length):
        mins, secs = divmod(length, 60)
        hours, mins = divmod(mins, 60)
        return '{}:{}:{}'.format(hours, mins, secs)

    def generate_playlist(self):
        playlist = Playlist('new playlist')
        data = [f for f in sorted(glob.glob('{}/*.mp3'
                                  .format(self.directory)))]
        for song in data:
            info = File(song)
            length = int(MP3(song).info.length)
            playlist.add_song(Song(info['TIT2'].text[0],
                                   info['TPE2'].text[0],
                                   info['TALB'].text[0],
                                   self.parse_length(length)))

        return playlist.pprint_playlist()


crawler = MusicCrawler('/home/gbojinov/Music/2008 - Death Magnetic')
print(crawler.generate_playlist())