from flask import Flask
from flask_cors import CORS
import datetime
import os
from time import sleep

app = Flask(__name__)
CORS(app, resources={r'*': {'origins': '*'}})

idx_map = [] # idx_map[0] 으로 접근 요망

idx_13hz = 0 # 13hz 의 인덱스를 얻어올 변수 (엑셀기준 - 2)
medium_arr_13hz = [] # 이중 배열 (13hz 기준 -2칸 +2칸, 총 5열)

idx_19hz = 0 # 19hz의 인덱스 (엑셀 기준 -2)
medium_arr_19hz = [] # 이중 배열 (19hz 기준 -2칸 +2칸, 총 5열)

idx_22hz = 0 # 22hz의 인덱스 (엑셀 기준 -2)
medium_arr_22hz = [] # 이중 배열 (22hz 기준 -2칸 +2칸, 총 5열)

time_stamp = [] 
# 일반적인 줄이 읽히는 시점이라면 0을 추가
# 중립평균 요청을 하면 -1 추가
# 첫번째 영역 요청을 하면 1추가,
# 두번째 영역 요청을 하면 2추가
# 세번째 영역 요청을 하면 3추가

m_avg_13hz = 0.00000000000000000000000 #13hz의 중립평균 초기화 (전체에서 비교할 때 사용할 중요한 값)
m_avg_19hz = 0.00000000000000000000000 #19hz의 중립평균 초기화
m_avg_22hz = 0.00000000000000000000000 #22hz의 중립평균 초기화

flag_1='' # 첫번째 res
flag_2='' # 두번째 res
flag_3='' # 세번째 res

@app.route('/')
def main():
    global idx_13hz
    global idx_19hz
    global idx_22hz

    # MAVE 녹화 시작 -> app.py와 동시에 실시간 라이브로 한줄씩 추가
    d = datetime.datetime.now()
    year = str(d.year)
    month = '0' + str(d.month)
    day = str(d.day)
    noonOrnight = '오전' if d.hour <= 12 else '오후'
    hour = str(d.hour) if d.hour <= 12 else str(d.hour - 12)
    minute = '0' + str(d.minute) if d.minute <= 9 else str(d.minute)  # 1~9분사이 폴더가 01, 02 분식으로 만들어짐
    k = year + '-' + month + '-' + day + '_' + noonOrnight + ' ' + hour + '_' + minute
    k_2 = '2022-06-11_오후 10_37'

    filePath = os.path.join('C:\MAVE_RawData', k, 'FP1_FFT.txt')
    with open(filePath, "r") as f:
        f.seek(0)
        while True:
            where = f.tell()
            line = f.readline().strip()
            if not line:
                sleep(0.1)
                delay_time += 0.1
                f.seek(where)
                if delay_time > 10.0: # 10초 이상 지연되면 파일 출력이 끝난 것으로 간주
                    break
            else:
                delay_time = 0.  # reset delay time
                # sleep(1)
                temp = line.split('\t')
                if len(idx_map) == 0: # 처음 읽힘과 동시, idx를 idx_map에 저장
                    idx_map.append(temp[1:]) #time column 삭제
                    idx_13hz = idx_map[0].index('13.00Hz')
                    idx_19hz = idx_map[0].index('19.00Hz')
                    idx_22hz = idx_map[0].index('22.00Hz')
                    print('idx_13hz',idx_13hz, 'idx_19hz', idx_19hz, 'idx_22hz',idx_22hz)
                else : # 두번째 줄 이후부터는, all_lines가 아닌 각각의 중립평균을 위한 이차원 배열에 저장된다. &저장 시점부터 엑셀과 동일한 자료형으로 저장
                    idx_temp = temp[1:] #time columne 삭제 
                    medium_arr_13hz.append([float(idx_temp[idx_13hz-2]),float(idx_temp[idx_13hz-1]), float(idx_temp[idx_13hz]), float(idx_temp[idx_13hz+1]), float(idx_temp[idx_13hz+2])])
                    medium_arr_19hz.append([float(idx_temp[idx_19hz-2]),float(idx_temp[idx_19hz-1]), float(idx_temp[idx_19hz]), float(idx_temp[idx_19hz+1]), float(idx_temp[idx_19hz+2])])
                    medium_arr_22hz.append([float(idx_temp[idx_22hz-2]),float(idx_temp[idx_22hz-1]), float(idx_temp[idx_22hz]), float(idx_temp[idx_22hz+1]), float(idx_temp[idx_22hz+2])])
                    time_stamp.append(0) #한줄 추가될 때마다 배열에 0 요소 추가
                    # print(medium_arr_13hz)
                    # print(len(time_stamp))

# 20초간 중립 평균을 구하는 라우팅 함수
@app.route('/mid_average')
def mid_request_0():
    print('중립평균 요청 옴')
    time_stamp.append(-1) #요청이 들어오면 time_stamp 리스트에 -1을 추가
    start = time_stamp.index(-1) #추가하는 동시에 time_stamp에서 -1의 index를 추가
    print('start', start, type(start))
    sleep(20) # 20초간 가만히
    end = len(medium_arr_13hz)-1
    sum_13hz = 0.0000000000000
    sum_19hz = 0.0000000000000
    sum_22hz = 0.0000000000000
    
    for y in range(start, end): #세로로 쌓이는 한줄에 대한 루프 기준점
        for x in range(0,5): 
            sum_13hz += medium_arr_13hz[y][x]
            sum_19hz += medium_arr_19hz[y][x]
            sum_22hz += medium_arr_22hz[y][x]
    

    temp = float((end-start) * 5)
    global m_avg_13hz 
    m_avg_13hz = sum_13hz / (temp)
    global m_avg_19hz
    m_avg_19hz = sum_19hz / (temp)
    global m_avg_22hz
    m_avg_22hz = sum_22hz / (temp)

    print('중립평균', m_avg_13hz, m_avg_19hz, m_avg_22hz)

    # print('end',end,sum_19hz,end-start)
    return {"result": {
        '13Hz 중립평균': m_avg_13hz, 
        '19Hz 중립평균': m_avg_19hz,
        '22Hz 중립평균' : m_avg_22hz,
    }}

@app.route('/first')
def mid_request_1():
    print('첫번째 요청이 왔습니다.')
    time_stamp.append(1) #요청이 들어오면 time_stamp 리스트에 1을 추가
    start = time_stamp.index(1) #추가하는 동시에 time_stamp에서 1의 index를 추가
    print('첫번째 요청 start', start)
    sleep(30) # 30초간 가만히
    end = len(medium_arr_13hz)-1
    print('첫번째 요청 end', end)
    global flag_1
    sum_13hz = 0.00000000000000000000000
    sum_19hz = 0.00000000000000000000000
    sum_22hz = 0.00000000000000000000000

    # 지역 변수 - 평균
    avg_13hz = 0.00000000000000000000000
    avg_19hz = 0.00000000000000000000000
    avg_22hz = 0.00000000000000000000000

    red_flag = 0.000000000000000000000000
    blue_flag = 0.000000000000000000000000
    green_flag = 0.000000000000000000000000

    for y in range(start, end): #세로로 쌓이는 한줄에 대한 루프 기준점
        for x in range(0,5): 
            sum_13hz += medium_arr_13hz[y][x]
            sum_19hz += medium_arr_19hz[y][x]
            sum_22hz += medium_arr_22hz[y][x]

    temp = float((end-start) * 5)
    print('첫번째 나눗셈 분모', temp)
    avg_13hz = sum_13hz / temp
    avg_19hz = sum_19hz / temp
    avg_22hz = sum_22hz / temp

    red_flag = avg_13hz - m_avg_13hz
    green_flag = avg_19hz - m_avg_19hz
    blue_flag = avg_22hz - m_avg_22hz

    print('첫번째 red_flag:',red_flag, 'green_flag',green_flag, 'blue_flag',blue_flag)

    if((red_flag >= blue_flag) and (red_flag >= green_flag)):
        flag_1='1-Red'
    elif((blue_flag >= red_flag) and (blue_flag >= green_flag)):
        flag_1='1-Blue'
    elif((green_flag >= red_flag) and (green_flag >= blue_flag)):
        flag_1='1-Green'
    else:
        flag_1='1-Error'

    return {"result": {
        'flag_1': flag_1
    }}

@app.route('/second')
def mid_request_2():
    time_stamp.append(2) #요청이 들어오면 time_stamp 리스트에 1을 추가
    start = time_stamp.index(2) #추가하는 동시에 time_stamp에서 1의 index를 추가
    print('두번째 요청 start', start)
    sleep(30) # 30초간 가만히
    end = len(medium_arr_13hz)-1
    print('두번째 요청 end', end)
    
    global flag_2

    sum_13hz = 0.00000000000000000000000
    sum_19hz = 0.00000000000000000000000
    sum_22hz = 0.00000000000000000000000

    avg_13hz = 0.00000000000000000000000
    avg_19hz = 0.00000000000000000000000
    avg_22hz = 0.00000000000000000000000

    red_flag = 0.000000000000000000000000
    green_flag = 0.000000000000000000000000
    blue_flag = 0.000000000000000000000000

    for y in range(start, end): #세로로 쌓이는 한줄에 대한 루프 기준점
        for x in range(0,5): 
            sum_13hz += medium_arr_13hz[y][x]
            sum_19hz += medium_arr_19hz[y][x]
            sum_22hz += medium_arr_22hz[y][x]

    temp = float((end-start)*5) 
    print('두번째 나눗셈 분모', temp)
    avg_13hz = sum_13hz / temp
    avg_19hz = sum_19hz / temp
    avg_22hz = sum_22hz / temp


    red_flag = avg_13hz - m_avg_13hz
    green_flag = avg_19hz - m_avg_19hz
    blue_flag = avg_22hz - m_avg_22hz

    print('두번째 red_flag:',red_flag, 'green_flag',green_flag, 'blue_flag',blue_flag)

    # compare_arr = []
    # compare_arr.append(red_flag, green_flag, blue_flag)
    
    if((red_flag >= blue_flag) and (red_flag >= green_flag)):
        flag_2 = flag_1 + '-' + 'Red'
    elif((blue_flag >= red_flag) and (blue_flag >= green_flag)):
        flag_2 = flag_1 + '-' + 'Blue'
    elif((green_flag >= red_flag) and (green_flag >= blue_flag)):
        flag_2 = flag_1 + '-' + 'Green'
    else:
        flag_2 = flag_1 + '-' + 'Error'


    return {"result": {
        'flag_2': flag_2
    }}

@app.route('/third')
def mid_request_3():
    time_stamp.append(3) #요청이 들어오면 time_stamp 리스트에 3을 추가
    start = time_stamp.index(3) #추가하는 동시에 time_stamp에서 3의 index를 추가
    print('세번째 요청 start', start)
    sleep(30) # 30초간 가만히
    end = len(medium_arr_13hz)-1
    print('세번째 요청 end', end)

    
    global flag_3

    sum_13hz = 0.00000000000000000000000
    sum_19hz = 0.00000000000000000000000
    sum_22hz = 0.00000000000000000000000

    avg_13hz = 0.00000000000000000000000
    avg_19hz = 0.00000000000000000000000
    avg_22hz = 0.00000000000000000000000

    red_flag = 0.000000000000000000000000
    blue_flag = 0.000000000000000000000000
    green_flag = 0.000000000000000000000000

    for y in range(start, end): #세로로 쌓이는 한줄에 대한 루프 기준점
        for x in range(0,5): 
            sum_13hz += medium_arr_13hz[y][x]
            sum_19hz += medium_arr_19hz[y][x]
            sum_22hz += medium_arr_22hz[y][x]

    temp = float((end-start)*5)
    print('세번째 나눗셈 분모', temp)
    avg_13hz = sum_13hz / temp
    avg_19hz = sum_19hz / temp
    avg_22hz = sum_22hz / temp

    red_flag = avg_13hz - m_avg_13hz
    green_flag = avg_19hz - m_avg_19hz
    blue_flag = avg_22hz - m_avg_22hz

    print('세번째 red_flag:',red_flag, 'green_flag',green_flag, 'blue_flag',blue_flag)

    if((red_flag >= blue_flag) and (red_flag >= green_flag)):
        flag_3 = flag_2 + '-' + 'Red'
    elif((blue_flag >= red_flag) and (blue_flag >= green_flag)):
        flag_3 = flag_2 + '-' + 'Blue'
    elif((green_flag >= red_flag) and (green_flag >= blue_flag)):
        flag_3 = flag_2 + '-' + 'Green'
    else:
        flag_3 = flag_2 +'-' +'Error'

    return {"result": {
        'flag_3': flag_3
    }}

if __name__ == '__main__':
    app.run(debug=True)