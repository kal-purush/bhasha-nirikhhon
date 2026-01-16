from pytube import YouTube
from sys import argv

outputFolder = "C:/Users/user/Videos/YouTube Donloads"

link = argv[1]
yt = YouTube(link)
yd =  yt.streams.get_highest_resolution()
yd.download(outputFolder)