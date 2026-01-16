#!/usr/bin/env python
import subprocess
import os
import RPi.GPIO as GPIO
import time
from pydub import AudioSegment
from flask import Flask
talkie = Flask(__name__)

GPIO.setmode(GPIO.BCM)
GPIO.setup(17, GPIO.IN)
GPIO.setup(18, GPIO.OUT)

@talkie.route("/")
def index():
    return "Talkie Web Link !! </br> Use /post/phrase=xx&jingle=xx&lang=xxxx </br> For no jingle use jingle=0 </br> Available languages are : </br> DE : deutsch ; GB : english GB; US : english US; ES : spanish; FR : french; IT : italian </br> To check if server is alive : /hello </br> To check state of GPIO 17 : /gpio-read"

@talkie.route("/hello")
def hello():
    return "Hello World!!"
    
@talkie.route("/gpio-read")
def gpioread():
    try:
        result=GPIO.input(17)
    except Exception, e:
        return str(e)
    return 'Etat Gpio 17 : ' + str(result)
    
@talkie.route('/post/phrase=<phrase>&jingle=<jingle>&lang=<lang>')
def talkjingle(phrase,jingle='0',lang='FR'):
    try:
        language_dict = {"FR" : 'fr-FR',
            "US" : 'en-US',
           "GB" : 'en-GB',
           "DE" : 'de-DE',
           "ES" : 'es-ES',
           "IT" : 'it-IT'
        }
        language=language_dict[lang]
        phrase=phrase.encode('utf-8')
        cachepath=os.path.dirname(os.path.dirname(__file__))
        jinglepath=os.path.abspath(os.path.join(os.path.dirname(__file__), 'jingle'))
        file = 'tts'
        filename=os.path.join(cachepath,file+'.wav')
        filenamemp3=os.path.join(cachepath,file+'.mp3')
        os.system('pico2wave -l '+language+' -w '+filename+ ' "' +phrase+ '"')
        song = AudioSegment.from_wav(filename)
        if not jingle or jingle=='0':
            songmodified=song
        else:
            jinglename=os.path.join(jinglepath,jingle+'.mp3')
            try:
                os.stat(jinglename)
            except:
                return 'Erreur le jingle %s n\'existe pas' % jinglename
            jinglename=os.path.join(jinglepath,jingle+'.mp3')
            jingle= AudioSegment.from_mp3(jinglename)
            songmodified = jingle+song
        songmodified.export(filenamemp3, format="mp3", bitrate="128k", tags={'albumartist': 'Talkie', 'title': 'TTS', 'artist':'Talkie'}, parameters=["-ar", "44100","-vol", "100"])
        song = AudioSegment.from_mp3(filenamemp3)
        cmd = ['mplayer']
        cmd.append(filenamemp3)
        if GPIO.input(17) != 0 :
            print 'GPIO 17 en cours d\'utilisation'
            while GPIO.input(17) != 0 :
                time.sleep(0.5)
        print 'GPIO 17 libre'
        GPIO.output(18, 1)
        print 'GPIO 18 ON et synthese du message'
        with open(os.devnull, 'wb') as nul:
            subprocess.call(cmd, stdout=nul, stderr=subprocess.STDOUT)
        GPIO.output(18, 0)
        print 'Synthese finie GPIO 18 OFF'
    except Exception, e:
        return str(e)
    return 'Post %s' % phrase

@talkie.route('/post/phrase=<phrase>&lang=<lang>')
def talklang(phrase,lang='FR'):
    try:
        language_dict = {"FR" : 'fr-FR',
            "US" : 'en-US',
           "GB" : 'en-GB',
           "DE" : 'de-DE',
           "ES" : 'es-ES',
           "IT" : 'it-IT'
        }
        language=language_dict[lang]
        phrase=phrase.encode('utf-8')
        cachepath=os.path.dirname(os.path.dirname(__file__))
        file = 'tts'
        filename=os.path.join(cachepath,file+'.wav')
        filenamemp3=os.path.join(cachepath,file+'.mp3')
        os.system('pico2wave -l '+language+' -w  '+filename+ ' "' +phrase+ '"')
        song = AudioSegment.from_wav(filename)
        songmodified=song
        songmodified.export(filenamemp3, format="mp3", bitrate="128k", tags={'albumartist': 'Talkie', 'title': 'TTS', 'artist':'Talkie'}, parameters=["-ar", "44100","-vol", "200"])
        song = AudioSegment.from_mp3(filenamemp3)
        cmd = ['mplayer']
        cmd.append(filenamemp3)
        if GPIO.input(17) != 0 :
            print 'GPIO 17 en cours d\'utilisation'
            while GPIO.input(17) != 0 :
                time.sleep(0.5)
        print 'GPIO 17 libre'
        GPIO.output(18, 1)
        print 'GPIO 18 ON et synthese du message'
        with open(os.devnull, 'wb') as nul:
            subprocess.call(cmd, stdout=nul, stderr=subprocess.STDOUT)
        GPIO.output(18, 0)
        print 'Synthese finie GPIO 18 OFF'
    except Exception, e:
        return str(e)
    return 'Post %s' % phrase


@talkie.route('/post/phrase=<phrase>')
def talk(phrase):
    try:
        phrase=phrase.encode('utf-8')
        cachepath=os.path.dirname(os.path.dirname(__file__))
        file = 'tts'
        filename=os.path.join(cachepath,file+'.wav')
        filenamemp3=os.path.join(cachepath,file+'.mp3')
        os.system('pico2wave -l fr-FR -w '+filename+ ' "' +phrase+ '"')
        song = AudioSegment.from_wav(filename)
        songmodified=song
        songmodified.export(filenamemp3, format="mp3", bitrate="128k", tags={'albumartist': 'Talkie', 'title': 'TTS', 'artist':'Talkie'}, parameters=["-ar", "44100","-vol", "200"])
        song = AudioSegment.from_mp3(filenamemp3)
        cmd = ['mplayer']
        cmd.append(filenamemp3)
        if GPIO.input(17) != 0 :
            print 'GPIO 17 en cours d\'utilisation'
            while GPIO.input(17) != 0 :
                time.sleep(0.5)
        print 'GPIO 17 libre'
        GPIO.output(18, 1)
        print 'GPIO 18 ON et synthese du message'
        with open(os.devnull, 'wb') as nul:
            subprocess.call(cmd, stdout=nul, stderr=subprocess.STDOUT)
        GPIO.output(18, 0)
        print 'Synthese finie GPIO 18 OFF'
    except Exception, e:
        return str(e)
    return 'Post %s' % phrase

if __name__ == "__main__":
    try:
        talkie.run(host='0.0.0.0',port=4001)
    except KeyboardInterrupt:
        print 'Ctrl+C pressed cleaning'
        GPIO.cleanup()
    except:
        GPIO.cleanup()
    finally:
        GPIO.cleanup()
        
        