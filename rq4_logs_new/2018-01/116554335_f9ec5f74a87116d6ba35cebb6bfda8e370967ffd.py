import os
import random as r

a = r.randint(1, 5)


if a == 1:
          print("""
 ------------------------
| Linux Installer v1.0   |
 ------------------------
| by BufferMuffin        |
|                        |
| --No System is Safe--  |
|                        |
 ------------------------ 
 
    ,-(|)--(|)-.
   \_   ..   _/
     \______/
       V  V                                  ____
       `.^^`.                               /^,--`
         \^^^\                             (^^\
         |^^^|                  _,-._       \^^\
        (^^^^\      __      _,-'^^^^^`.    _,'^^)
         \^^^^`._,-'^^`-._.'^^^^__^^^^ `--'^^^_/
          \^^^^^ ^^^_^^^^^^^_,-'  `.^^^^^^^^_/ hjw
           `.____,-' `-.__.'        `-.___.'   `97



 
          """)


if a == 2:
    print("""
    
 ------------------------
| Linux Installer v1.0   |
 ------------------------
| by BufferMuffin        |
|                        |
| --No System is Safe--  |
|                        |
 ------------------------ 
  
     _____
   .'     `.
  /  .-=-.  \   \ __
  | (  C\ \  \_.'')
 _\  `--' |,'   _/
/__`.____.'__.-'

  
""")

if a == 3:
    print("""

 ------------------------
| Linux Installer v1.0   |
 ------------------------
| by BufferMuffin        |
|                        |
| --No System is Safe--  |
|                        |
 ------------------------ 

     _>-.    ____    .-<_
      >-.\.-'____`-./ ,<
        .','"\__/"`.`<_,-,
      _/ / \_/  \_/ \!  (D\
       \ \_/ \__/ \_/!_ (D/
        `.`../__\,.'.< `-`
     _>-'/`-.____.-'\ `<_
      >-'            `-<
hjw


""")

if a == 4:
    print("""

             ------------------------
            | Linux Installer v1.0   |
             ------------------------
            | by BufferMuffin        |
            |                        |
            | --No System is Safe--  |
            |                        |
             ------------------------ 

@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@
@@@@@@@@@@@@@@@@@@@@@@@'~~~     ~~~`@@@@@@@@@@@@@@@@@@@@@@@@@
@@@@@@@@@@@@@@@@@@'                     `@@@@@@@@@@@@@@@@@@@@
@@@@@@@@@@@@@@@'                           `@@@@@@@@@@@@@@@@@
@@@@@@@@@@@@@'                               `@@@@@@@@@@@@@@@
@@@@@@@@@@@'                                   `@@@@@@@@@@@@@
@@@@@@@@@@'                                     `@@@@@@@@@@@@
@@@@@@@@@'                                       `@@@@@@@@@@@
@@@@@@@@@                                         @@@@@@@@@@@
@@@@@@@@'                      n,                 `@@@@@@@@@@
@@@@@@@@                     _/ | _                @@@@@@@@@@
@@@@@@@@                    /'  `'/                @@@@@@@@@@
@@@@@@@@a                 <~    .'                a@@@@@@@@@@
@@@@@@@@@                 .'    |                 @@@@@@@@@@@
@@@@@@@@@a              _/      |                a@@@@@@@@@@@
@@@@@@@@@@a           _/      `.`.              a@@@@@@@@@@@@
@@@@@@@@@@@a     ____/ '   \__ | |______       a@@@@@@@@@@@@@
@@@@@@@@@@@@@a__/___/      /__\ \ \     \___.a@@@@@@@@@@@@@@@
@@@@@@@@@@@@@/  (___.'\_______)\_|_|        \@@@@@@@@@@@@@@@@
@@@@@@@@@@@@|\________                       ~~~~~\@@@@@@@@@@
""")

if a == 5:
    print("""

 ------------------------
| Linux Installer v1.0   |
 ------------------------
| by BufferMuffin        |
|                        |
| --No system is safe--  |
|                        |
 ------------------------ 
 
                ___,A.A_  __
               \   ,   7"_/
                ~"T(  r r)
                  | \    Y
                  |  ~\ .|
                  |   |`-'
                  |   |
                  |   |
                  |   |
                  |   |
                  j   l
                 /     \
                Y       Y
                l   \ : |
                /\   )( (
               /  \  I| |\
              Y    I l| | Y
              j    ( )( ) l
             / .    \ ` | |\   -Row
            Y   \    i  | ! Y
            l   .\_  I  |/  |
             \ /   [\[][]/] j
          ~~~~~~~~~~~~~~~~~~~~~~~



""")

print("""
a) Get Kali-Repository (supported)

b) Show list of tools

c) Tweak System

d) Help

e) Info

""")

input1 = input("==>")

if input1 == "a":
          print('''
   
    1)Get Kali-Repositories 
    
    2)Remove all Kali-Repositories
   
    3) View Kali content of the sources.list file   
     
''')
          inpt2 = input("==>")
          if inpt2 == "1":
             os.system ( "apt-key adv --keyserver pgp.mit.edu --recv-keys ED444FF07D8D0BF6" )
             os.system ("echo '# Kali linux repositories | Added by Linux-Installer\ndeb http://http.kali.org/kali kali-rolling main contrib non-free' >> /etc/apt/sources.list" )
             os.system("apt-get install apt-transport-https")
             os.system("apt-get update && apt-get upgrade")

          elif inpt2 == "2":
              i_file = "/etc/apt/sources.list"
              o_file = "/etc/apt/sources.list"

              delete_kalirepo = ["# Kali linux repositories | Added by Katoolin\n",
                             "deb http://http.kali.org/kali kali-rolling main contrib non-free\n"]
              fi = open (i_file)
              os.remove ( "/etc/apt/sources.list" )
              fo = open(o_file, "w+")
              for line in fi:
                  for word in delete_kalirepo:
                      line = line.replace ( word, "" )
                  fo.write(line)
              fi.close ()
              fo.close ()
              print ( "All Kali-Repositories have been deleted!" )

          elif inpt2 == "3":
              sl = open('/etc/apt/sources.list', 'r')

              print(sl.read())



if input1 == "b":
               print("""

a) Exploitation Tools

b) Information Gathering

c) Wireless Attacks

d) Web Applications

e) Stress Testing

f) Forenensics Tools""")
               inpt = input("==> ")

               if inpt == "a":


                                 print("""
What tool do you wanna download?

(1) Armitage
(2) Metasploit
(3) Backdoor Factory
(4) BeEF
(5) cisco-auditing-tool
(6) cisco-global-exploiter	
(7) cisco-ocs
(8) cisco-torch
(9) commix
(10) crackle
(11) jboss-autopwn
(12) Linux Exploit Suggester
(13) Maltego Teeth
(14) SET
(15) ShellNoob
(16) sqlmap
(17) THC-IPV6
(18) Yersinia  
(19) Katana-Framework            (0)install all
                              
                                 """)

                                 inpt1 = input("==>")
                                 if inpt1 == "1":
                                                 os.system("apt-get install armitage")
                           
                                 elif inpt1 == "2":
                                                   os.system("apt-get install curl && curl https://raw.githubusercontent.com/rapid7/metasploit-omnibus/master/config/templates/metasploit-framework-wrappers/msfupdate.erb > msfinstall && chmod 755 msfinstall && ./msfinstall")
                      
                                 elif inpt1 == "3":
                                                   os.system("apt-get install backdoor-factory")


                                 elif inpt1 == "4":
                                                   os.system ("apt-get install beef-xss")




                                 elif inpt1 == "5":
                                                   os.system ("apt-get install cisco-auditing-tool")



                                 elif inpt1 == "6":
                                                  os.system ("apt-get install cisco-global-exploiter")


                                 elif inpt1 == "7":
                                                  os.system ("apt-get install cisco-ocs")


                                 elif inpt1 == "8":
                                                  os.system ("apt-get install cisco-torch")


                                 elif inpt1 == "9":
                                                  os.system ("apt-get install git & & git clone https//github.com/stasinopoulos/commix.git commix & & cd commix & & python./commix.py - -install")


                                 elif inpt1 == "10":
                                                  os.system ("apt-get install crackle")


                                 elif inpt1 == "11":
                                                  os.system ("apt-get install jboss-autopwn")


                                 elif inpt1 == "12":
                                                  os.system ("apt-get install linux-exploit-suggester")


                                 elif inpt1 == "13":
                                                  os.system ("apt-get install maltego-teeth")

                                 elif inpt1 == "14":
                                                  os.system ("apt-get install set")

                                 elif inpt1 == "15":
                                                  os.system ("apt-get install shellnoob")

                                 elif inpt1 == "16":
                                                  os.system ("apt-get install sqlmap")

                                 elif inpt1 == "17":
                                                  os.system ("apt-get install thc-ipv6")

                                 elif inpt1 == "18":
                                                  os.system ("apt-get install yersinia")

                                 elif inpt1 == "19":
                                                  os.system ("apt-get install git && git clone https://github.com/PowerScript/KatanaFramework.git")
                                                  print("""
                                                  !!!!!!!!!!!!!!!!!!!!!!!!!!!!!!
                                                  TYPE IN:
                                                  cd ... KatanaFramework 
                                                  sudo sh dependencies 
                                                  sudo python install 
                                                  ktf.console""")

                                 elif inpt1 == "0":
                                                  os.system ("apt-get install -y armitage backdoor-factory cisco-auditing-tool cisco-global-exploiter cisco-ocs cisco-torch crackle jboss-autopwn linux-exploit-suggester maltego-teeth set shellnoob sqlmap thc-ipv6 yersinia beef-xss")
                                 else:
                                      print("This command is not avaible!")

               elif inpt == "b":
                             print("""
                   
         --Information Gathering--          
                   
                   
 1) acccheck					30) lbd                    58)kickthemout
 2) SSLyze					    31) Maltego Teeth
 3) Amap			     		32) DMitry
 4) Automater					33) Metagoofil
 5) bing-ip2hosts				34) Miranda
 6) SET  				    	35) Nmap
 7) CaseFile					36) Operative
 8) CDPSnarf					37) p0f
 9) cisco-torch					38) Parsero
10) Cookie Cadger				39) Recon-ng
11) copy-router-config	 	    40) braa
12) masscan				        41) smtp-user-enum
13) dnmap				        42) snmpcheck
14) dnsenum				        43) sslcaudit
15) dnsmap				        44) SSLsplit
16) DNSRecon					45) sslstrip
17) dnstracer					46) ace-voip
18) dnswalk					    47) THC-IPV6
19) URLCrazy					48) theHarvester
20) enum4linux					49) TLSSLed
21) enumIAX					    50) Firewalk
22) exploitdb					51) DotDotPwn
23) Fierce				     	52) Wireshark
24) twofi    					53) WOL-E
25) fragroute					54) Xplico
26) fragrouter					55) iSMTP
27) hping3       				56) InTrace
28) GoLismero					57) Ghost Phisher          (0)install all
""")


                             input2 = input("==>")
                             if input2 == "1":
                                   os.system ( "apt-get install acccheck" )
                                   print("Test")

                             elif input2 == "2":
                                     os.system ( "apt-get install sslyze" )

                             elif input2 == "3":
                                     os.system ( "apt-get install amap" )

                             elif input2 == "4":
                                     os.system ( "apt-get install automater" )

                             elif input2 == "5":
                                     os.system ("wget http://www.morningstarsecurity.com/downloads/bing-ip2hosts-0.4.tar.gz && tar -xzvf bing-ip2hosts-0.4.tar.gz && cp bing-ip2hosts-0.4/bing-ip2hosts /usr/local/bin/")

                             elif input2 == "6":
                                     os.system ( "apt-get install set" )

                             elif input2 == "7":
                                     os.system ( "apt-get install casfile" )

                             elif input2 == "8":
                                     os.system ( "apt-get install cdpsnarf" )

                             elif input2 == "9":
                                     os.system ( "apt-get install cisco-torch" )

                             elif input2 == "10":
                                     os.system ( "apt-get install cookie-cadger" )

                             elif input2 == "11":
                                     os.system ( "apt-get install copy-router-config" )

                             elif input2 == "12":
                                     os.system ( "apt-get install masscan" )

                             elif input2 == "13":
                                     os.system ("apt-get install dnmap")

                             elif input2 == "14":
                                     os.system ( "apt-get install dnsenum" )

                             elif input2 == "15":
                                     os.system ( "apt-get install dnsmap" )

                             elif input2 == "16":
                                     os.system ( "apt-get install dnsrecon" )

                             elif input2 == "17":
                                     os.system ( "apt-get install dnstracer" )

                             elif input2 == "18":
                                     os.system ( "apt-get install dnswalk" )

                             elif input2 == "19":
                                     os.system ( "apt-get install dotdotpwn" )

                             elif input2 == "20":
                                     os.system ( "apt-get install enum4linux" )

                             elif input2 == "21":
                                     os.system ( "apt-get install enumiax" )

                             elif input2 == "22":
                                     os.system ( "apt-get install exploitdb" )

                             elif input2 == "23":
                                     os.system ( "apt-get install fierce" )

                             elif input2 == "24":
                                     os.system ( "apt-get install twofi" )

                             elif input2 == "25":
                                     os.system ( "apt-get install fragroute" )

                             elif input2 == "26":
                                     os.system ( "apt-get install fragrouter" )

                             elif input2 == "27":
                                     os.system ( "apt-get install hping3" )

                             elif input2 == "28":
                                     os.system ( "apt-get install golismero" )

                             elif input2 == "29":
                                     os.system ( "apt-get install goofile" )

                             elif input2 == "30":
                                     os.system ( "apt-get install lbd" )

                             elif input2 == "31":
                                     os.system ( "apt-get install maltego-teeth" )

                             elif input2 == "32":
                                     os.system ( "apt-get install dmitry" )

                             elif input2 == "33":
                                     os.system ( "apt-get install metagoofil" )

                             elif input2 == "34":
                                     os.system ( "apt-get install miranda" )

                             elif input2 == "35":
                                     os.system ( "apt-get install nmap" )

                             elif input2 == "36":
                                     os.system ( "git clone https://github.com/graniet/operative-framework && cd operative-framework && pip install -r requirements.txt" )
                                     print("EXECUTE WITH : chmod +x operative.py")

                             elif input2 == "37":
                                     os.system ( "apt-get install p0f" )

                             elif input2 == "38":
                                     os.system ( "apt-get install parsero" )

                             elif input2 == "39":
                                     os.system ( "apt-get install recon-ng" )

                             elif input2 == "40":
                                    os.system ( "apt-get install braa" )

                             elif input2 == "41":
                                    os.system ( "apt-get install smtp-user-enum" )

                             elif input2 == "42":
                                    os.system ( "apt-get install snmpcheck" )

                             elif input2 == "43":
                                    os.system ( "apt-get install sslcaudit" )

                             elif input2 == "44":
                                    os.system ( "apt-get install sslsplit" )

                             elif input2 == "45":
                                   os.system ( "apt-get install sslstrip" )

                             elif input2 == "46":
                                   os.system ( "apt-get install ace-voip" )

                             elif input2 == "47":
                                   os.system ( "apt-get install thc-ipv6" )

                             elif input2 == "48":
                                   os.system ( "apt-get install theharvester" )

                             elif input2 == "49":
                                   os.system ( "apt-get install tlssled" )

                             elif input2 == "50":
                                   os.system ( "apt-get install firewalk" )

                             elif input2 == "51":
                                   os.system ( "apt-get install urlcrazy" )

                             elif input2 == "52":
                                   os.system ( "apt-get install wireshark" )

                             elif input2 == "53":
                                   os.system ( "apt-get install wol-e" )

                             elif input2 == "54":
                                   os.system ( "apt-get install xplico" )

                             elif input2 == "55":
                                     os.system ( "apt-get install ismtp" )

                             elif input2 == "56":
                                     os.system ( "apt-get install intrace" )

                             elif input2 == "57":
                                     os.system ( "apt-get install ghost-phisher" )

                             elif input2 == "58":
                                     os.system ("apt-get install git && git clone https://github.com/k4m4/kickthemout")
                                     print("""
                                     
                                     
                                     !!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!
                                     !                                                    !
                                     ! type in:   cd kickthemout                          !
                                     !                                                    !
                                     !             sudo pip install -r requirements.txt   !
                                     !                                                    !
                                     !             chmod +x kicktemout.py                 !
                                     !                                                    !
                                     !           ./kickthemout.py or python kickthemout.py!
                                     !                                                    !
                                     !!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!
                                     
                                     
                                     
                                     
                                     
                                     """)
                             elif input2 == "0":
                                     os.system ( "apt-get -f install acccheck ace-voip amap automater braa casefile cdpsnarf cisco-torch cookie-cadger copy-router-config dmitry dnmap dnsenum dnsmap dnsrecon dnstracer dnswalk dotdotpwn enum4linux enumiax exploitdb fierce firewalk fragroute fragrouter ghost-phisher golismero goofile lbd maltego-teeth masscan metagoofil miranda nmap p0f parsero recon-ng set smtp-user-enum snmpcheck sslcaudit sslsplit sslstrip sslyze thc-ipv6 theharvester tlssled twofi urlcrazy wireshark wol-e xplico ismtp intrace hping3 bbqsql bed cisco-auditing-tool cisco-global-exploiter cisco-ocs cisco-torch copy-router-config doona dotdotpwn greenbone-security-assistant hexorbase jsql lynis nmap ohrwurm openvas-cli openvas-manager openvas-scanner oscanner powerfuzzer sfuzz sidguesser siparmyknife sqlmap sqlninja sqlsus thc-ipv6 tnscmd10g unix-privesc-check yersinia aircrack-ng asleap bluelog blueranger bluesnarfer bully cowpatty crackle eapmd5pass fern-wifi-cracker ghost-phisher giskismet gqrx kalibrate-rtl killerbee kismet mdk3 mfcuk mfoc mfterm multimon-ng pixiewps reaver redfang spooftooph wifi-honey wifitap wifite apache-users arachni bbqsql blindelephant burpsuite cutycapt davtest deblaze dirb dirbuster fimap funkload grabber jboss-autopwn joomscan jsql maltego-teeth padbuster paros parsero plecost powerfuzzer proxystrike recon-ng skipfish sqlmap sqlninja sqlsus ua-tester uniscan vega w3af webscarab websploit wfuzz wpscan xsser zaproxy burpsuite dnschef fiked hamster-sidejack hexinject iaxflood inviteflood ismtp mitmproxy ohrwurm protos-sip rebind responder rtpbreak rtpinsertsound rtpmixsound sctpscan siparmyknife sipp sipvicious sniffjoke sslsplit sslstrip thc-ipv6 voiphopper webscarab wifi-honey wireshark xspy yersinia zaproxy cryptcat cymothoa dbd dns2tcp http-tunnel httptunnel intersect nishang polenum powersploit pwnat ridenum sbd u3-pwn webshells weevely casefile cutycapt dos2unix dradis keepnote magictree metagoofil nipper-ng pipal armitage backdoor-factory cisco-auditing-tool cisco-global-exploiter cisco-ocs cisco-torch crackle jboss-autopwn linux-exploit-suggester maltego-teeth set shellnoob sqlmap thc-ipv6 yersinia beef-xss binwalk bulk-extractor chntpw cuckoo dc3dd ddrescue dumpzilla extundelete foremost galleta guymager iphone-backup-analyzer p0f pdf-parser pdfid pdgmail peepdf volatility xplico dhcpig funkload iaxflood inviteflood ipv6-toolkit mdk3 reaver rtpflood slowhttptest t50 termineter thc-ipv6 thc-ssl-dos acccheck burpsuite cewl chntpw cisco-auditing-tool cmospwd creddump crunch findmyhash gpp-decrypt hash-identifier hexorbase john johnny keimpx maltego-teeth maskprocessor multiforcer ncrack oclgausscrack pack patator polenum rainbowcrack rcracki-mt rsmangler statsprocessor thc-pptp-bruter truecrack webscarab wordlists zaproxy apktool dex2jar python-distorm3 edb-debugger jad javasnoop jd ollydbg smali valgrind yara android-sdk apktool arduino dex2jar sakis3g smali && wget http://www.morningstarsecurity.com/downloads/bing-ip2hosts-0.4.tar.gz && tar -xzvf bing-ip2hosts-0.4.tar.gz && cp bing-ip2hosts-0.4/bing-ip2hosts /usr/local/bin/" )

                             else:
                                     print("This command is not avaible")



               elif inpt == "c":
                   print("""
                                 ---WIRELESS-ATTACKS---
                                 
                                 
                      1) Aircrack-ng		16) kalibrate-rtl
                      2) Asleap			    17) KillerBee
                      3) Bluelog		    18) Kismet
                      4) BlueMaho		    19) mdk3
                      5) Bluepot		    20) mfcuk
                      6) BlueRanger		    21) mfoc
                      7) Bluesnarfer		22) mfterm
                      8) Bully		       	23) Multimon-NG
                      9) coWPAtty		    24) PixieWPS
                     10) crackle		    25) Reaver
                     11) eapmd5pass		    26) redfang
                     12) Fern Wifi Cracker	27) RTLSDR Scanner  
                     13) GISKismet          28) Spooftooph    
                     14) gr-scan            29) Wifi Honey	 
                     15) Wifite             30) Wifitap	 (0)install all
        	          
                   """)

                   input3= input("==>")
                   if input3 == 1:
                      os.system("apt-get install aircrack-ng")
                   elif input3 == 2:
                      os.system("apt-get install asleap")
                   elif input3 == 3:
                      os.system("apt-get install bluelog")
                   elif input3 == 4:
                      os.system("apt-get install git && git clone git://git.kali.org/packages/bluemaho.git")
                   elif input3 == 5:
                       os.system ( "apt-get install git && git clone git://git.kali.org/packages/bluepot.git" )
                   elif input3 == 6:
                       os.system ( "apt-get install blueranger" )
                   elif input3 == 7:
                       os.system ( "apt-get install bluesnarfer" )
                   elif input3 == 8:
                       os.system ( "apt-get install bully" )
                   elif input3 == 9:
                       os.system ( "apt-get install cowpatty" )
                   elif input3 == 10:
                       os.system ( "apt-get install crackle" )
                   elif input3 == 11:
                       os.system ( "apt-get install eapmd5pass" )
                   elif input3 == 12:
                       os.system ( "apt-get install fern-wifi-cracker" )
                   elif input3 == 13:
                       os.system ( "apt-get install giskismet" )
                   elif input3 == 14:
                       os.system ( "apt-get install apt-get install git && git clone git://git.kali.org/packages/gr-scan.git" )
                   elif input3 == 15:
                       os.system ( "apt-get install kalibrate-rtl" )
                   elif input3 == 16:
                      os.system("apt-get install killerbee")
                   elif input3 == 17:
                      os.system("apt-get install kismet")
                   elif input3 == 18:
                      os.system("apt-get install mdk3")
                   elif input3 == 19:
                      os.system("apt-get install mfcuk")
                   elif input3 == 20:
                       os.system ( "apt-get install mfoc" )
                   elif input3 == 21:
                      os.system("apt-get install mfterm")
                   elif input3 == 22:
                      os.system("apt-get install multimon-ng")
                   elif input3 == 23:
                      os.system("apt-get install pixiewps")
                   elif input3 == 24:
                      os.system("apt-get install reaver")
                   elif input3 == 25:
                       os.system ( "apt-get install redfang" )
                   elif input3 == 26:
                      os.system("apt-get install rtlsdr-scanner")
                   elif input3 == 27:
                      os.system("apt-get install spooftooph")
                   elif input3 == 28:
                      os.system("apt-get install wifi-honey")
                   elif input3 == 29:
                      os.system("apt-get install wifitap")
                   elif input3 == 0:
                       os.system ( "apt-get install -y aircrack-ng asleap bluelog blueranger bluesnarfer bully cowpatty crackle eapmd5pass fern-wifi-cracker giskismet gqrx kalibrate-rtl killerbee kismet mdk3 mfcuk mfoc mfterm multimon-ng pixiewps reaver redfang spooftooph wifi-honey wifitap wifite")
                   else:
                       print("This command is not avaible!")

               elif inpt == "d":


                    print("  -+-+-+-+-+-+-Web Applications-+-+-+-+-+-+-")
                    print( "1) apache-users			        21) Parsero")
                    print( "2) Arachni				22) plecost")
                    print( "3) BBQSQL				23) Powerfuzzer")
                    print("4) BlindElephant		        24) ProxyStrike")
                    print("5) Burp Suite		    	        25) Recon-ng")
                    print("6) commix				26) Skipfish")
                    print("7) CutyCapt				27) sqlmap")
                    print("8) DAVTest				28) Sqlninja")
                    print("9) deblaze				29) sqlsus")
                    print("10) DIRB				30) ua-tester")
                    print("11) DirBuster			        31) Uniscan")
                    print("12) fimap				32) Vega")
                    print("13) FunkLoad				33) w3af")
                    print("14) Grabber				34) WebScarab")
                    print("15) jboss-autopwn			35) Webshag")
                    print("16) joomscan				36) WebSlayer")
                    print("17) jSQL				37) WebSploit")
                    print("18) Maltego Teeth			38) Wfuzz")
                    print("19) PadBuster			        39) WPScan")
                    print("20) Paros				40) XSSer")
                    print("					41) zaproxy")
                    print("                 42)TorGhost")
                    print("0) Install all Web Applications tools")

                    input4 = input("==>")
                    if input4 == "1":
                        os.system("apt-get install apache-users")
                    elif input4 == "2":
                        os.system("apt-get install arachni")
                    elif input4 == "3":
                        os.system("apt-get install bbqsql")
                    elif input4 == "4":
                        os.system("apt-get install blindelephant")
                    elif input4 == "5":
                        os.system("apt-get install burpsuite")
                    elif input4 == "6":
                        os.system("apt-get install cutycapt")
                    elif input4 == "7":
                        os.system("apt-get install git && git clone https://github.com/stasinopoulos/commix.git commix && cd commix && python ./commix.py --install")
                    elif input4 == "8":
                        os.system("apt-get install davtest")
                    elif input4 == "9":
                        os.system("apt-get install deblaze")
                    elif input4 == "10":
                        os.system("apt-get install dirb")
                    elif input4 == "11":
                        os.system("apt-get install dirbuster")
                    elif input4 == "12":
                        os.system("apt-get install fimap")
                    elif input4 == "13":
                        os.system("apt-get install funkload")
                    elif input4 == "14":
                        os.system("apt-get install grabber")
                    elif input4 == "15":
                        os.system("apt-get install jboss-autopwn")
                    elif input4 == "16":
                        os.system("apt-get install joomscan")
                    elif input4 == "17":
                        os.system("apt-get install jsql")
                    elif input4 == "18":
                        os.system("apt-get install maltego-teeth")
                    elif input4 == "19":
                        os.system("apt-get install padbuster")
                    elif input4 == "20":
                        os.system("apt-get install paros")
                    elif input4 == "21":
                        os.system("apt-get install parsero")
                    elif input4 == "22":
                        os.system("apt-get install plecost")
                    elif input4 == "23":
                        os.system("apt-get install powerfuzzer")
                    elif input4 == "24":
                        os.system("apt-get install proxystrike")
                    elif input4 == "25":
                        os.system("apt-get install recon-ng")
                    elif input4 == "26":
                        os.system("apt-get install skipfish")
                    elif input4 == "27":
                        os.system("apt-get install sqlmap")
                    elif input4 == "28":
                        os.system("apt-get install sqlninja")
                    elif input4 == "29":
                        os.system("apt-get install sqlsus")
                    elif input4 == "30":
                        os.system("apt-get install ua-tester")
                    elif input4 == "31":
                        os.system("apt-get install uniscan")
                    elif input4 == "32":
                        os.system("apt-get install vega")
                    elif input4 == "33":
                        os.system("apt-get install w3af")
                    elif input4 == "34":
                        os.system("apt-get install webscarab")
                    elif input4 == "35":
                        print ( "Webshag is unavailable" )
                    elif input4 == "36":
                        os.system ( "apt-get install git && git clone git://git.kali.org/packages/webslayer.git" )
                    elif input4 == "37":
                        os.system("apt-get install websploit")
                    elif input4 == "38":
                        os.system("apt-get install wfuzz")
                    elif input4 == "39":
                        os.system("apt-get install wpscan")
                    elif input4 == "40":
                        os.system("apt-get install xsser")
                    elif input4 == "41":
                        os.system("apt-get install zaproxy")
                    elif input4 == "42":
                        os.system("apt-get install git && git clone https://github.com/susmithHCK/torghost.git")
                    elif input4 == "0":
                        os.system("apt-get install -y apache-users arachni bbqsql blindelephant burpsuite cutycapt davtest deblaze dirb dirbuster fimap funkload grabber jboss-autopwn joomscan jsql maltego-teeth padbuster paros parsero plecost powerfuzzer proxystrike recon-ng skipfish sqlmap sqlninja sqlsus ua-tester uniscan vega w3af webscarab websploit wfuzz wpscan xsser zaproxy")
                    else:
                        print("This command is not avaible!")


               elif inpt == "e":
                   print ( '''
                    ----Stress Testing----
                    1) DHCPig
                    2) FunkLoad
                    3) iaxflood
                    4) Inundator
                    5) inviteflood	
                    6) ipv6-toolkit
                    7) mdk3
                    8) Reaver
                    9) rtpflood
                   10) SlowHTTPTest
                   11) t50
                   12) Termineter
                   13) THC-IPV6
                   14) THC-SSL-DOS 		
                   15) Ufonet
                   0) Install all Stress Testing tools

                   						''' )
                   input5 = input("==> ")
                   if input5 == "1":
                       os.system("apt-get install dhcpig")
                   elif input5 == "2":
                        os.system("apt-get install funkload")
                   elif input5 == "3":
                        os.system("apt-get install iaxflood")
                   elif input5 == "4":
                       os.system("apt-get install git && git clone git://git.kali.org/packages/inundator.git")
                   elif input5 == "5":
                       os.system("apt-get install inviteflood")
                   elif input5 == "6":
                       os.system("apt-get install ipv6-toolkit")
                   elif input5 == "7":
                        os.system("apt-get install mdk3")
                   elif input5 == "8":
                        os.system("apt-get install reave")
                   elif input5 == "9":
                       os.system("apt-get install rtpflood")
                   elif input5 == "10":
                       os.system("apt-get install slowhttptest")
                   elif input5 == "11":
                       os.system("apt-get install t50")
                   elif input5 == "12":
                       os.system ("apt-get install termineter")
                   elif input5 == "13":
                       os.system ("apt-get install thc-ipv6")
                   elif input5 == "14":
                       os.system ("apt-get install thc-ssl-dos")
                   elif input5 == "15":
                       os.system ("apt-get install git && git clone https://github.com/epsylon/ufonet")
                   elif input5 == "0":
                       os.system ("apt-get install -y dhcpig funkload iaxflood inviteflood ipv6-toolkit mdk3 reaver rtpflood slowhttptest t50 termineter thc-ipv6 thc-ssl-dos")

                   else:
                       print("This command is not avaible!")



elif input1 == "c":
     print("""
   
     1) Make a terminal ascii headline
   
     2) Elementary Tweaks

   
      """)
     inpt3 = input("==>")
     if inpt3 == "1":
        os.system("apt-get install figlet")
        fobj = open ( "/root/.bashrc", "r+" )
        txt = input("header: ")
        print("header = " + txt)
        print("""
        What kind of font?
        
        (1)big
        (2)banner
        (3)normal
        (4)block
        (5)bubble
        
        """)
        choice = input("==>")
        if choice == "1":
           fobj.write("figlet -f big " +"\""+ txt +"\"\n")
        elif choice == "2":
           fobj.write("figlet -f banner " +"\""+ txt +"\"\n")
        elif choice == "3":
            fobj.write ( "figlet " + "\"" + txt + "\"\n" )
        elif choice == "4":
            fobj.write ( "figlet -f block " + "\"" + txt + "\"\n" )
        elif choice == "5":
            fobj.write ( "figlet -f bubble " + "\"" + txt + "\"\n" )

        print("""
        2nd line?
        
        (y)es
        (n)o
        """)
        choice2 = input("==>")
        if choice2 == "y":
           txt2 = input("Text: ")
           fobj.write("echo " + txt2 +"\n")
           print("done...")
        elif choice2 == "n":
            print("done...")
        else:
            print("This command is not avaible! Type   y  or  n")


elif input1 == "d":
                print("""

                       ---HELP---

First of all you need to know that you should execute the first step:
!!!!!a) Get Kali-Repository (supported)!!!!!

Executing files:  .sh = 1) chmod +x file.sh
                        2) ./file.sh

                  .py = 1) python file.py
                        2) python3 file.py

                  .c = 1) gcc -o filename file.c
                       2) ./filename

If some aktions wont work you can contact me on facebook (BufferMuffin Yt)


""")

elif input1 == "e":
                print("""
                       ---INFO---
                    
This tool is 4 downloading penetest tools on linux.
It needs the repo of Kali Linux for some tools...
Thank you for your attention!                               
                                
                
                """)

else:
    print("This command is not avaible!")