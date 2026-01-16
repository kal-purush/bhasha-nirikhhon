# Python Port Scanner by Courtbox...





import socket
import time
openPort = 0
closedPort = 0





# Port scanner...
def scanPort(ipaddress, port):
    try:
        sock = socket.socket()
        sock.settimeout(0.5)
        time.sleep(sleepTime)
        sock.connect((ipaddress, port))
        
        # Open port notification...
        print('[+] PORT ' + str(port) + ' IS OPEN.')
        global openPort
        openPort += 1
        
    except:
        # Closed port notification...
        print('[-] PORT ' + str(port) + ' IS CLOSED.')
        global closedPort
        closedPort += 1





# Entering Target IP Address...
ipaddress = input('Example: 140.82.112.3\n\n[+] Enter the target IP address to scan: ')





# Type of port scan... (individual, range)...
scanQuestion = input('\n ind - Enter an individual port to check\n rang - Enter range of ports to check\n\n[+]: ')





# Individual port scan...
if scanQuestion == 'ind':
    port = input('\n[+] Enter port to check: ')
    time.sleep(0.5)
    scanPort(ipaddress, port)





# Range of ports to check...
elif scanQuestion == 'rang':
    
    # Entering range numbers...
    pr1 = eval(input('[1/2] Enter start of port range to check: '))
    pr2 = eval(input('[2/2] Enter end of port range to check: '))
    
    # Entering duration between scans...
    sleepTime = eval(input('\nTip: Longer time = more accurate results\n\n[+] Enter timeout between port checks (SECONDS): '))
    for port in range(pr1, (pr2 + 1)):
        scanPort(ipaddress, port)
    
    # Results of scan...
    print('\nPort scanner finised.\n  Open ports found:', openPort, 'out of', ((pr2-pr1) + 1) , '\n  Closed ports found:', closedPort, 'out of', ((pr2-pr1) + 1))





# If entered invalid statement...
else:
    print('[-] INVALID ENTRY.')