#!/usr/bin/env python

from tracks import VideoTrack, AudioTrack, SubTrack


class MediaInfo:
    def __init__(self, input):
        self.video, self.audio, self.subs = self.generate(input)
        print self.video, self.audio, self.subs

    def getvideo(self):
        return self.video

    def getaudio(self):
        return self.audio

    def getsubs(self):
        return self.subs

    @staticmethod
    def generate(input):
        videos, audios, subs = [], [], []
        inputs = input.split('\n\n')
        inputs_formatted = []
        for i in inputs:
            inputs_formatted.append(i.split('\n'))
        for i in inputs_formatted:
            title = i[0]
            if 'Video' in title:
                videos.append(VideoTrack(i, len(videos) + 1))
            elif 'Audio' in title:
                audios.append(AudioTrack(i, len(audios) + 1))
            elif 'Text' in title:
                subs.append(SubTrack(i, len(subs) + 1))
        return videos, audios, subs