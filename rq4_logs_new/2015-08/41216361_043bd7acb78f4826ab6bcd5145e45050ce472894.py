from fretboard.fretboard import get_index
from list_sound_files.list_sound_files import list_files
from subprocess import call
import argparse
import random
import sys
import time

from list_sound_files.list_sound_files import list_files
from fretboard.fretboard import get_index


class Fret:
    current_note = 0
    current_string = 0
    inclusive = False
    notes = ['E', 'F', 'F#/Gb', 'G', 'G#/Ab', 'A', 'A#/Bb', 'B', 'C',
             'C#/Db', 'D', 'D#/Eb']
    string_indices = [6, 5, 4, 3, 2, 1]
    string = ['6th', '5th', '4th', '3rd', '2nd', '1st']
    strings = [0, 1, 2, 3, 4, 5]
    order = [0, 3, 5, 7, 10, 1, 4, 6, 8, 9, 11, 2]
    files = []
    sequential_random_index = 0
    index_for_first_six = 0

    def zero_based_string_to_string_number(self, current_string):
        return self.string_indices[current_string]

    def get_file(self, current_string, note):
        return self.files[get_index(
            self.zero_based_string_to_string_number(current_string), note)]

    def get_note_and_file(self, current_string, day, notes):
        if self.inclusive:
            rand_n = random.randint(0, day - 1)
            note = notes[self.order[rand_n]]
            file = self.get_file(current_string, note)
        else:
            note = notes[self.order[day - 1]]
            file = self.get_file(current_string, note)

        return file, note

    def notes_by_day(self):
        p = ''
        for s in self.order:
            p += self.notes[s] + ' '
        return p

    def play_back_options(self, args):
        rate = '200'
        if args.level == 'beginner' or args.level == 'Lil_Wayne':
            playback = '1'
            rand_from = 2
            rand_to = 4
        elif args.level == 'easy' or args.level == 'Noel_Gallagher':
            playback = '1'
            rand_from = 1
            rand_to = 3
        elif args.level == 'medium' or args.level == 'Jimmy_Page':
            playback = '0.5'
            rand_from = 0
            rand_to = 0
        elif args.level == 'hard' or args.level == 'Hendrix':
            playback = '0.1'
            rand_from = 0
            rand_to = 0
            rate = '350'
        elif args.level == 'chuck_norris':
            playback = '0.01'
            rand_from = 0
            rand_to = 0
            rate = '450'
        return playback, rand_from, rand_to, rate

    def play_sound(self, file, playback):
        call(["afplay", "sounds/" + file, "-t", playback])

    def display_string_and_note(self, current_string, note):
        out = '%s string %s\r' % (self.string[current_string], note)
        return out

    def say_string_and_note(self, out, rate):
        out = out.replace('b', ' flat')
        call(["say", out, '--rate', rate])

    def sleep(self, rand_from, rand_to):
        time.sleep(random.uniform(rand_from, rand_to))

    def random_string(self):
        if self.sequential_random_index > 5:
            self.sequential_random_index = 0
            self.strings_tmp = list(self.strings)
        current_string = random.choice(self.strings_tmp)
        self.strings_tmp.remove(current_string)
        self.sequential_random_index += 1
        return current_string

    def get_current_string(self):
        if self.index_for_first_six > 5 or self.inclusive:
            current_string = self.random_string()
        else:
            current_string = self.index_for_first_six
            self.index_for_first_six += 1
        return current_string

    def __init__(self):
        self.strings_tmp = list(self.strings)

        self.files = list_files('sounds', 'Strat P- 52.wav')

        inclusive = False

        parser = argparse.ArgumentParser()
        parser.add_argument('-d', '--day', default=0)
        parser.add_argument('-l', '--level', default='easy')
        parser.add_argument('-i', '--inclusive', help="inclusive",
                            action="store_true")
        parser.add_argument('-s', '--sound', help="sound", action="store_true")
        args = parser.parse_args()

        day = int(args.day)

        if args.inclusive:
            self.inclusive = True
        else:
            self.inclusive = False

        if args.sound:
            sound = True
        else:
            sound = False

        playback, sleep_from, sleep_to, rate = self.play_back_options(args)

        print (self.notes_by_day())

        try:
            while True:

                current_string = self.get_current_string()

                file, note = self.get_note_and_file(current_string, day,
                                                    self.notes)

                out = self.display_string_and_note(current_string, note)

                print (out)

                self.say_string_and_note(out, rate)

                self.sleep(sleep_from, sleep_to)

                if sound:
                    self.play_sound(file, playback)

        except KeyboardInterrupt:
            sys.exit(0)


if __name__ == '__main__':
    fret = Fret()

    fret()