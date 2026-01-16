def toseconds(time):
    h, m, s = time.split(':')  #seperating hh:mm:ss
    return int(h) * 3600 + int(m) * 60 + float(s)    #converting to seconds

rps = float(input())
noc = int(input())
drofeach = list(input().split())
for i in range(len(drofeach)):
    time = toseconds(drofeach[i])
    cost = time*rps
    print("time in seconds : "+str(time)+ ",","cost is : "+str(cost))

