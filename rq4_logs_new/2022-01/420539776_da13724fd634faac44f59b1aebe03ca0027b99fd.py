from typing import List, Sequence, Optional, Union

from icecream import ic
from mido import MidiTrack, MidiFile

from MidiCompose.logic.harmony.chord import Chord
from MidiCompose.logic.melody.melody import Melody
from MidiCompose.logic.rhythm.beat import Beat
from MidiCompose.logic.rhythm.measure import Measure
from MidiCompose.logic.rhythm.part import Part
from MidiCompose.translation.track_builder import TrackBuilder


class ChordSequence:
    def __init__(self, chords: Optional[Sequence[Chord]] = None):
        self.chords = chords  # calls setter

    @property
    def chords(self):
        return self._chords

    @chords.setter
    def chords(self, value: Sequence[Chord]):

        if value is None:
            _chords = list()
        else:
            all_chords = all([isinstance(v, Chord) for v in value])
            if not all_chords:
                e = "Parameter `chords` must be a sequence of Chord instances."
                raise TypeError(e)

            _chords = value

        self._chords = _chords

    @property
    def max_notes(self) -> int:
        """
        Returns integer representing the number of notes in the chord with the most notes.
        """
        return max([len(c) for c in self.chords])

    @property
    def min_notes(self) -> int:
        return min([len(c) for c in self.chords])

    #### UTILITY METHODS ####
    def append_chord(self, chord: Chord):
        self.chords.append(chord)
        return self

    def insert_chord(self, idx: int, chord: Chord):
        if idx not in range(len(self) + 1):
            e = f"idx `{idx}` out of range for ChordSet with length {len(self)}"
            raise IndexError(e)
        else:
            self.chords.insert(idx, chord)
        return self

    def extend(self, chords: Sequence[Chord], idx: Optional[int] = None):

        if not all([isinstance(c, Chord) for c in chords]):
            print(" ".join([str(type(c for c in chords))]))
            e = "All elements in `chords` must be Chord instances."
            raise TypeError(e)

        if idx:
            for c in chords:
                self.insert_chord(idx, c)
                idx += 1
        else:
            self.chords.extend(chords)

        return self

    #### MIDI METHODS ####
    def quick_midi(self,
                   bpm: int = 60,
                   beats_per_chord: int = 4,
                   velocities: Union[int,Sequence[int]] = 64,
                   return_type: str = "MidiFile"
                   ) -> Union[MidiFile,MidiTrack]:
        """
        :param bpm: Tempo of output track (beats per minute)
        :param beats_per_chord: Number of beats per chord (one chord per measure)
        :param velocities: If single integer, all voices same volume, if sequence of integers, must be same length as \
        `self.max_notes`.
        :param return_type: Either "MidiFile" or "MidiTrack"
        """

        n_measures = len(self)  # one measure per chord
        n_parts = self.max_notes  # one part per chord voice

        # handle velocities
        if isinstance(velocities, int):  # all melodies same volume
            if not 0 <= velocities <= 127:
                e = f"Given velocity {velocities} out of range."
                raise ValueError(e)
            velocities = [velocities for _ in range(n_parts)]

        elif isinstance(velocities,Sequence):
            if len(velocities) != n_parts:
                e = "When passing a sequence of velocities, must give one velocity per 'chord voice'"
                raise ValueError(e)
            elif not all([0 <= v <= 127 for v in velocities]):
                e = "All velocities must be between 0 and 127."
                raise ValueError(e)

        # initialize empty parts/melodies
        _beats = Beat([2]) * beats_per_chord
        _measures = Measure(_beats) * n_measures

        parts = Part(_measures) * n_parts
        melodies = Melody() * n_parts

        # populate parts and melodies
        for i_chord, c in enumerate(self):
            len_chord = len(c)
            for i_part, p in enumerate(parts):
                if len_chord >= i_part + 1:
                    # activate first subdivision of first beat of current measure
                    p[i_chord][0][0].activate()
                    melodies[i_part].append_note(c[i_part])
                else:
                    p[i_chord][0][0].deactivate()

        # set velocities
        for m,v in zip(melodies,velocities):
            m.velocity = v

        # build track
        tb = TrackBuilder(parts=parts,melodies=melodies,bpm=bpm)

        track = tb.get_track()

        if return_type == "MidiTrack":
            return track

        else:
            # get midifile
            mid = MidiFile()
            mid.tracks.append(track)
            return mid

    def __getitem__(self, item: int):
        return self.chords[item]

    def __len__(self):
        return len(self.chords)

    def __repr__(self):
        s = "ChordSequence(\n" \
            "    [\n"
        for i, c in enumerate(self):
            if i == len(self) - 1:
                s += "    " + repr(c) + "\n"
            else:
                s += "    " + repr(c) + ",\n"
        s += "     ])"
        return s