import time
hours = 8
seconds = hours * 3600
count = 0
while count < seconds:
    print(count)
    if(count % 3600 == 0):
        print( str(count/3600) + " Hour has passed...")
    count += 1
    time.sleep(1)