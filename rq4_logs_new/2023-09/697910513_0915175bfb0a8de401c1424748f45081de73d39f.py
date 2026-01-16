import pyttsx3,datetime
from math import floor
import soundfile as sf
from pydub import AudioSegment
def save_audio(data, filename):
    sf.write(filename, data, 22050, 'PCM_16')
def use_pyttsx3():
    engine = pyttsx3.init()
    rate = engine.getProperty('rate')
    print(f'语音速率：{rate}')
    engine.setProperty('rate', 350)
    volume = engine.getProperty('volume')
    print(f'语音音量：{volume}')
    engine.setProperty('volume', 1.0)
    voices = engine.getProperty('voices')
    print(f'语音声音详细信息：{voices}')
    engine.setProperty('voice', voices[1].id)
    voice = engine.getProperty('voice')
    print(f'语音声音：{voice}','\n','\n','以下为输出的文本内容：')
    out=open('in.lrc','a')
    with open('in.txt', 'r') as file:
        t0=datetime.datetime.now()
        for line in file:
            print(line,end='')
            if line[-1]!='\n':
                print()
            t=datetime.datetime.now()
            t=t-t0
            s,ms=t.seconds,t.microseconds
            m=floor(s/60)
            ms=round(t.microseconds/10000)
            s=s-m*60
            if ms==100:
                ms=0
                s+=1
                if s==60:
                    s=0
                    m+=1
            m,s,ms=str(m),str(s),str(ms)
            if len(m)==1:
                m="0"+m
            if len(s)==1:
                s="0"+s
            if len(ms)==1:
                ms="0"+ms
            time="["+m+":"+s+":"+ms+"]"
            words = line
            engine.say(words)
            engine.runAndWait()
            engine.save_to_file(words, filename='C:/Users/Andmi/Desktop/wavfiles/go_out.wav', name='test')
            engine.runAndWait()
            out.write(time+words)
    file.close()
    out.close()
if __name__ == '__main__':
    use_pyttsx3()