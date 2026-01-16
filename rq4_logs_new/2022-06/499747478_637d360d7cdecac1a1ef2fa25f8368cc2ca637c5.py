# Make a countdown Timer
import time
endTime = int(input("Enter the end time in seconds: "))
timeLeft = endTime

for i in range(1,endTime+1):
    timeLeft = endTime - i
    time.sleep(1)
    print(f'Count: {i} seconds && Time Left: {timeLeft} seconds')
    if(i == endTime):
        print("Time's up!")