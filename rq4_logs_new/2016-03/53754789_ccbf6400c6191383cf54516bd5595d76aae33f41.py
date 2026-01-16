from mutagen.mp3 import MP3
from mutagen.flac import FLAC
from mutagen.easyid3 import EasyID3
import mutagen.id3


import os
import sys
#import property_reader

if len(sys.argv) > 1:
	substringTitle = int(sys.argv[1])
	print("Remove first " + str(substringTitle) + " caracter.")
else:
	substringTitle = 0

for subdir, dirs, files in os.walk('./'):
	for file in files:
		plop = file.split(".")
		if plop[1] == "flac":
			print ("file found : " + plop[0])
			os.system("vlc -I dummy \"" + plop[0] + ".flac\" \":sout=#transcode{acodec=mpga,ab=192}:std{dst='" + plop[0][substringTitle:] + ".mp3',access=file}\" vlc://quit")
			# MP3
			mf = MP3(plop[0] + ".mp3", ID3=EasyID3)
			#FLAC
			ff = FLAC(plop[0] + ".flac")
			#Let's go !
			mf['title'] = ff['title']
			mf['artist'] = ff['artist']
			mf['album'] = ff['album']
			mf['date'] = ff['date']
			mf['genre'] = ff['genre']
			mf['tracknumber'] = ff['tracknumber']
			mf.save()
			