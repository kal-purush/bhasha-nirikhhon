import os
try:
    from fake_useragent import UserAgent
except:
    os.system("pip install fake-useragent")
brow=["chrome", "edge", "internet explorer", "firefox", "safari", "opera"]


def run():
    global brow
    os.system("clear")
    try:
        browser=int(input("[1]Chrome\n[2]Edge\n[3]Internet Explorer\n[4]Firefox\n[5]Safari\n[6]Opera\nChoose: "))
    except:
        browser=1
    uas=[]
    ua = UserAgent(browsers=[brow[browser-1]])
    try:
        lim=int(input("Total User Agent: "))
    except:
        lim=1000
    file=input("Save File Path: ")
    if file=="":
        file="/sdcard/ua.txt"
    open (file,"w").truncate()
    print ("Generating user agents. wait for a while...")
    for i in range(lim):
        open(file,"a").write(ua.random+"\n")
    print(f"Saved in {file}\n")
    input ("Press enter to continue ")
    #print(len(uas))
    
while True:
    run()
    
    