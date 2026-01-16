import datetime
import os
from time import sleep

def main():
    d = datetime.datetime.now()

    year = str(d.year)
    month = '0'+str(d.month)
    day = '0'+str(d.day)
    noonOrnight = '오전' if d.hour<=12 else '오후'
    hour = str(d.hour) if d.hour<=12 else str(d.hour-12)
    minute = str(d.minute)
    l=[]
    k = year + '-' + month + '-' + day + '_' + noonOrnight+' '+ hour + '_' + minute
    k_2= '2022-05-23_오전 11_07'

    # filePath=os.path.join('C:\MAVE_RawData', k_2, 'FP1_FFT.txt')

    filePath=os.path.join('C:\MAVE_RawData', k, 'FP1_FFT.txt')
    # print(filePath)
    l = []


    with open(filePath, "r") as f:
        f.seek(0)
        while True:
            where = f.tell()
            line = f.readline().strip()
            if not line:
                sleep(0.1)
                delay_time += 0.1
                f.seek(where)
                if delay_time > 10.0:  # 10초 이상 지연되면 파일 출력이 끝난 것으로 간주

                    break
                # print('대기중')
            else:
                delay_time = 0. # reset delay time
                print(line)  # already has newline
                l.append(line)




    # f= open(filePath, 'r')
    # line=f.readlines()
    #
    # for i in range(len(line)):
    #     print(line[i], end = ' ')


main()