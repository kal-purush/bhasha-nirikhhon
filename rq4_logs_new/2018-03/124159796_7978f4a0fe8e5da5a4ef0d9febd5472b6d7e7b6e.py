# -*- coding: UTF-8 -*-
import wave, pyaudio
from datetime import datetime
import json, base64, uuid, os
import wave
import pycurl
import io
import urllib2
import getpass, email, sys
import imaplib
import smtplib
from email.mime.text import MIMEText
import psycopg2
from time import *
import getpass

#实时录音模块的准备
CHUNK = 1024 # 缓存块的大小
FORMAT = pyaudio.paInt16 # 取样值的量化格式
RATE = 8000  # 取样频率，百度语音识别库指定8000
CHANNELS = 1 # 声道数，百度语音识别库指定1
RECORD_SECONDS = 10 # 时间段，秒

#调百度API的准备
bda_access_token = ""
bda_expires_in = ""
ret_text = ""

#-----------------------------------------------------录音部分-----------------------
#参数为空（存储在当前目录），返回值为音频文件存储路径
def record_wave(to_dir=None):
    if to_dir is None:
        to_dir = "./"

    pa = pyaudio.PyAudio()
    
    # format 取样值的量化格式
    # channels 声道数
    # rate 取样频率，一秒内对声音信号的采集次数
    # input 输入流标志
    # frames_per_buffer 底层缓存块的大小
    stream = pa.open(format = FORMAT,
                     channels = CHANNELS,
                     rate = RATE,
                     input = True,
                     frames_per_buffer = CHUNK)

    print("* recording")

    # 
    save_buffer = []
    for i in range(0, int(RATE / CHUNK * RECORD_SECONDS)):
        audio_data = stream.read(CHUNK)
        save_buffer.append(audio_data)

    print("* done recording")
    print("Please wait...")

    # stop
    stream.stop_stream()
    stream.close()
    pa.terminate()

    # wav path
    file_name = datetime.now().strftime("%Y-%m-%d_%H_%M_%S")+".wav"
    if to_dir.endswith('/'):
        file_path = to_dir + file_name
    else:
        file_path = to_dir + "/" + file_name

    # save file
    wf = wave.open(file_path, 'wb')
    wf.setnchannels(CHANNELS)
    wf.setsampwidth(pa.get_sample_size(FORMAT))
    wf.setframerate(RATE)
    
    # 注意join 前的类型，如果是str类型会出错
    wf.writeframes(b''.join(save_buffer))
    wf.close()

    return file_path



#-----------------------------------------------------调百度API部分------------------
def get_mac_address():
    return uuid.UUID(int=uuid.getnode()).hex[-12:]








# 获取百度token(自己的key和secret在url里面)
def get_access_token():
    url = "https://openapi.baidu.com/oauth/2.0/token?grant_type=client_credentials&client_id=4Ukt2VqoazpazykizHuY68Zj&client_secret=58c77974a48bde2fbf4c3380af0f7c42"

    req = urllib2.Request(url)
    resp = urllib2.urlopen(req)
    data = resp.read().decode('utf-8')
    json_data = json.loads(data)

    global bda_access_token
    bda_access_token = json_data['access_token']

    return bda_access_token

# 读取wav文件内容
def get_wav_data(wav_path):
    if wav_path is None or len(wav_path) == 0:
        return None

    # 使用"rb"(二进制模式)打开文件
    wav_file = wave.open(wav_path, 'rb')
    nframes = wav_file.getnframes()
    audio_data = wav_file.readframes(nframes)

    return audio_data, nframes
    
    
# 解析返回值
def dump_res(buf):
    resp_json = json.loads(buf.decode('utf-8'))
    ret = resp_json['result']

    global ret_text
    ret_text = ret[0]

    #print(buf)



#转换函数，没有参数，实时录音，返回音频内容文本
def wav_to_text():
    #实时录音
    wav_path=record_wave()

    if len(bda_access_token) == 0:
        get_access_token()
        if len(bda_access_token) == 0:
            return None

    data, f_len = get_wav_data(wav_path)

    url = 'http://vop.baidu.com/server_api?dev_pid=1736&cuid=' + get_mac_address() + '&token=' + bda_access_token
    
    # 必须是list，不能是dict
    http_header = [
        'Content-Type: audio/pcm; rate=8000',
        'Content-Length: %d' % f_len
    ]

    c = pycurl.Curl()
    
    # url
    c.setopt(pycurl.URL, str(url)) 
    
    # header
    c.setopt(c.HTTPHEADER, http_header) 
    
    # Method
    c.setopt(c.POST, 1) 
    
    # 连接超时时间
    c.setopt(c.CONNECTTIMEOUT, 30) 
    
    # 请求超时时间
    c.setopt(c.TIMEOUT, 30)
    
    # 返回信息回调
    c.setopt(c.WRITEFUNCTION, dump_res)
    
    # post 的信息
    c.setopt(c.POSTFIELDS, data)
    
    # post的信息长度
    c.setopt(c.POSTFIELDSIZE, f_len)
    
    c.perform() 

    #删除录音文件
    os.remove(wav_path)

    print("Is the following sentence correct? If it is accurate, please waiting for reply, if not, please try again or using e-mail.")
    print ret_text

    return ret_text



def send_mail(mail_user,mail_pass,sub): 
    
    
    mail_host= "smtp.gmail.com" 
    #mail_user= "smart.meeting.agent@gmail.com"   
    #mail_pass=  "Monday123" 
    mail_port="587"
    
    mail_msg = " "
    
    #邮件其他相关量
    me= mail_user
    msg = MIMEText(
    mail_msg, 'plain', 'utf-8'
    )
    msg['Subject'] = sub  
    msg['From'] = me  
    msg['To'] ="smart.meeting.agent@gmail.com"
    try:  
        server = smtplib.SMTP(mail_host,mail_port) 
        server.ehlo()
        server.starttls()
        server.login(mail_user,mail_pass)  
        server.sendmail(me, "smart.meeting.agent@gmail.com", msg.as_string())  
        server.quit()  
        return True  
    except Exception, e:  
        print str(e)  
        return False



def main():
    
    
    mail_user=raw_input('mail: ')
    mail_pass=getpass.getpass('password: ')
    send_mail(mail_user,mail_pass,wav_to_text())

    

if __name__ == '__main__':
    main()