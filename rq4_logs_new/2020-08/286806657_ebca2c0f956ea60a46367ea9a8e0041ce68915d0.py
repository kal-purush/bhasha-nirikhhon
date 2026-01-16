import os
import wave
import alsaaudio
import threading
from gtts import gTTS
from pydub import AudioSegment

language = 'de'



class Audio:
    def __init__(self):
        self.language = "de"
        self.CHUNK = 320
        self.currentFileName = ""

    def textToSpeech( self, text, fileName ):
        print("Converting text to speach")
        obj = gTTS(text=text, lang=self.language, slow=False)
        print("Now save it")
        obj.save(fileName[:-3] + "mp3")
        print("Now convert it %s" % fileName[:-3] + "mp3")
        self.convertMP3ToWav(fileName[:-3] + "mp3", fileName)

    def convertMP3ToWav(self, inName, outName):
        sound = AudioSegment.from_mp3(inName)
        print("Export as wav now %s" % outName)
        sound.export(outName, format="wav")

    
    def play_file( self, fileName ):
        print("Playing file: %s" %fileName)
        self._thread = threading.Thread(target=self._play_file, args=(fileName, ), daemon=True)
        print("Now start thread:")
        self._thread.start()
        self.playing_audio = True

    def _play_file( self, fileName ):
        self.stop_audio = False
        print("Thread started for audio file")
        f = wave.open(fileName, "rb")
        if not f:
            print("File %s not found or playable" %fileName)
            pass

        stream = alsaaudio.PCM(type=alsaaudio.PCM_PLAYBACK, 
            mode=alsaaudio.PCM_NORMAL, periodsize=self.CHUNK , device="plughw:1,0")
        stream.setchannels(f.getnchannels())
        stream.setrate(f.getframerate())
        if f.getsampwidth() == 1:
            stream.setformat(alsaaudio.PCM_FORMAT_U8)
        # Otherwise we assume signed data, little endian
        elif f.getsampwidth() == 2:
            stream.setformat(alsaaudio.PCM_FORMAT_S16_LE)
        elif f.getsampwidth() == 3:
            stream.setformat(alsaaudio.PCM_FORMAT_S24_LE)
        elif f.getsampwidth() == 4:
            stream.setformat(alsaaudio.PCM_FORMAT_S32_LE)
        else:
            raise ValueError('Unsupported format')
        



        data = f.readframes(self.CHUNK)

        ##Play it:
        while data and not self.stop_audio:
            stream.write(data)
            data = f.readframes(self.CHUNK)