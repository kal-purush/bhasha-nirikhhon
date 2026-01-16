import datetime
import os
from time import sleep

all_lines = []  # line 읽어 넣을 배열, 최종적으로 이중 배열이 된다.
idx_7hz = 0
idx_13hz = 0
idx_19hz = 0
cnt = 0
m_avg_7hz = 0.00000000000000000000000
m_avg_13hz = 0.00000000000000000000000
m_avg_19hz = 0.00000000000000000000000
flag_1=''
flag_2=''
flag_3=''


# @app.route('/') : Front_end 쪽에서 호출
def main():

    global cnt
    d = datetime.datetime.now()
    year = str(d.year)
    month = '0' + str(d.month)
    day = '0' + str(d.day)
    noonOrnight = '오전' if d.hour <= 12 else '오후'
    hour = str(d.hour) if d.hour <= 12 else str(d.hour - 12)
    minute = '0' + str(d.minute) if d.minute <= 9 else str(d.minute)  # 1~9분사이 폴더가 01, 02 분식으로 만들어짐
    k = year + '-' + month + '-' + day + '_' + noonOrnight + ' ' + hour + '_' + minute
    k_2 = ''

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
                temp = line.split('\t')
                all_lines.append((temp))
                cnt+=1 # 라인 수

# 20초 정도(대기화면에서 idx를 미리받아옴. )
def request_idx():

    global idx_7hz
    global idx_13hz
    global idx_19hz


    # index는 어차피 300 이하
    for i in range(500):
        if all_lines[0][i] == '7.00Hz':  # 첫 행의 열의 인덱스를 구한다. 첫 행에는 hz 성분이 저장된다.
            idx_7hz = i
        if all_lines[0][i] == '13.00Hz':
            idx_13hz = i
        if all_lines[0][i] == '19.00Hz':
            idx_19hz = i
            break

    return {"result": {
        'idx_7hz':  idx_7hz,
        'idx_13hz': idx_13hz,
        'idx_19hz': idx_19hz

    }}





# @app.route('/mid_average') Front_end 쪽에서 40초후 호출 , 40~60초간 라인 측정후 중립평균 보냄.

# 3841 : 코드짠 사람 컴퓨터의 엑셀 열이 3841 까지있었으며, 컴퓨터에 따라 더 늘어날 수 잇으나 13,19,23 hz등의 상대적으로 저주파 성분을
# 인덱싱 하기 때문에 길이는 상관없음.
def mid_request_0():

    global cnt
    cnt_first = cnt
    cnt_req_0 = cnt
    lines_7hz = []
    lines_13hz = []
    lines_19hz = []


    d = datetime.datetime.now()
    d_m = d.minute
    d_s = d.second
    d_m_first = d_m*60 + d_s

    sum_7hz =  0.00000000000000000000000
    sum_13hz = 0.00000000000000000000000
    sum_19hz = 0.00000000000000000000000

    global m_avg_7hz
    global m_avg_13hz
    global m_avg_19hz

    # 40~60s 간 돌아감. (Append), cnt_req_0가 cnt보다 늘어나는 속도가 빠를 것으로 예상, if문 추가
    while(True):
            if(cnt_req_0> cnt):
                sleep(0.1)
            d_2 = datetime.datetime.now()
            d_m_now = d_2.minute * 60 + d_2.second
            for i in range(-2,3):
                lines_7hz.append(all_lines[cnt_req_0][idx_7hz+i])
                lines_13hz.append(all_lines[cnt_req_0][idx_13hz+i])
                lines_19hz.append(all_lines[cnt_req_0][idx_19hz+i])
                cnt_req_0+=1

            if(d_m_first - d_m_now > 20):
                    break


    for i in range(cnt_first, cnt_req_0+1):
        sum_7hz += lines_7hz[i]
        sum_13hz += lines_13hz[i]
        sum_19hz += lines_19hz[i]

    m_avg_7hz = sum_7hz/float(cnt_req_0+1 - cnt_first)
    m_avg_13hz = sum_13hz/float(cnt_req_0 + 1 - cnt_first)
    m_avg_19hz = sum_19hz/float(cnt_req_0 + 1 - cnt_first)


    return {"result": {
        # '13hz 중립' : avg_13hz,
        # '19hz 중립' : avg_19hz,
        # '23hz 중립' : avg_23hz,
        'avg_7hz': m_avg_7hz,
        'avg_13hz': m_avg_13hz,
        'avg_19hz': m_avg_19hz

    }}


def mid_request_1():
    global cnt
    cnt_first = cnt
    cnt_req_0 = cnt
    lines_7hz = []
    lines_13hz = []
    lines_19hz = []

    d = datetime.datetime.now()
    d_m = d.minute
    d_s = d.second
    d_m_first = d_m * 60 + d_s

    sum_7hz = 0.00000000000000000000000
    sum_13hz = 0.00000000000000000000000
    sum_19hz = 0.00000000000000000000000

    avg_7hz = 0.00000000000000000000000
    avg_13hz = 0.00000000000000000000000
    avg_19hz = 0.00000000000000000000000

    red_flag = 0.000000000000000000000000
    blue_flag = 0.000000000000000000000000
    green_flag = 0.000000000000000000000000

    global flag_1


    global m_avg_7hz
    global m_avg_13hz
    global m_avg_19hz

    # 호출시점부터 30s 간 돌아감. (Append)
    while (True):
        if (cnt_req_0 > cnt):
            sleep(0.1)

        d_2 = datetime.datetime.now()
        d_m_now = d_2.minute * 60 + d_2.second
        for i in range(-2, 3):
            lines_7hz.append(all_lines[cnt_req_0][idx_7hz + i])
            lines_13hz.append(all_lines[cnt_req_0][idx_13hz + i])
            lines_19hz.append(all_lines[cnt_req_0][idx_19hz + i])
            cnt_req_0 += 1

        if (d_m_first - d_m_now > 30):
            break

    for i in range(cnt_first, cnt_req_0 + 1):
        sum_7hz += lines_7hz[i]
        sum_13hz += lines_13hz[i]
        sum_19hz += lines_19hz[i]

    avg_7hz = sum_7hz/float(cnt_req_0+1-cnt_first)
    avg_13hz = sum_13hz/float(cnt_req_0+1-cnt_first)
    avg_19hz = sum_19hz/float(cnt_req_0+1-cnt_first)

    red_flag = avg_7hz - m_avg_7hz
    blue_flag = avg_13hz - m_avg_13hz
    green_flag = avg_19hz - m_avg_19hz

    if((red_flag >= blue_flag) and (red_flag >= green_flag)):
        flag_1='1-Red'
    elif((blue_flag >= red_flag) and (blue_flag >= green_flag)):
        flag_1='1-Blue'
    elif((green_flag >= red_flag) and (green_flag >= blue_flag)):
        flag_1='1-Green'


    return {"result": {
        'flag_1': flag_1

    }}

def mid_request_2():
    global cnt
    cnt_first = cnt
    cnt_req_0 = cnt
    lines_7hz = []
    lines_13hz = []
    lines_19hz = []

    d = datetime.datetime.now()
    d_m = d.minute
    d_s = d.second
    d_m_first = d_m * 60 + d_s

    sum_7hz = 0.00000000000000000000000
    sum_13hz = 0.00000000000000000000000
    sum_19hz = 0.00000000000000000000000

    avg_7hz = 0.00000000000000000000000
    avg_13hz = 0.00000000000000000000000
    avg_19hz = 0.00000000000000000000000

    red_flag = 0.000000000000000000000000
    blue_flag = 0.000000000000000000000000
    green_flag = 0.000000000000000000000000

    global flag_1
    global flag_2


    global m_avg_7hz
    global m_avg_13hz
    global m_avg_19hz

    # 호출시점부터 30s 간 돌아감. (Append)
    while (True):
        if (cnt_req_0 > cnt):
            sleep(0.1)
        d_2 = datetime.datetime.now()
        d_m_now = d_2.minute * 60 + d_2.second
        for i in range(-2, 3):
            lines_7hz.append(all_lines[cnt_req_0][idx_7hz + i])
            lines_13hz.append(all_lines[cnt_req_0][idx_13hz + i])
            lines_19hz.append(all_lines[cnt_req_0][idx_19hz + i])
            cnt_req_0 += 1

        if (d_m_first - d_m_now > 30):
            break

    for i in range(cnt_first, cnt_req_0 + 1):
        sum_7hz += lines_7hz[i]
        sum_13hz += lines_13hz[i]
        sum_19hz += lines_19hz[i]

    avg_7hz = sum_7hz/float(cnt_req_0+1-cnt_first)
    avg_13hz = sum_13hz/float(cnt_req_0+1-cnt_first)
    avg_19hz = sum_19hz/float(cnt_req_0+1-cnt_first)

    red_flag = avg_7hz - m_avg_7hz
    blue_flag = avg_13hz - m_avg_13hz
    green_flag = avg_19hz - m_avg_19hz

    if((red_flag >= blue_flag) and (red_flag >= green_flag)):
        flag_2=flag_1+'-'+'Red'
    elif((blue_flag >= red_flag) and (blue_flag >= green_flag)):
        flag_2=flag_1+'-'+'Blue'
    elif((green_flag >= red_flag) and (green_flag >= blue_flag)):
        flag_2=flag_1+'-'+'Green'


    return {"result": {
        'flag_2': flag_2

    }}

def mid_request_3():
    global cnt
    cnt_first = cnt
    cnt_req_0 = cnt
    lines_7hz = []
    lines_13hz = []
    lines_19hz = []

    d = datetime.datetime.now()
    d_m = d.minute
    d_s = d.second
    d_m_first = d_m * 60 + d_s

    sum_7hz = 0.00000000000000000000000
    sum_13hz = 0.00000000000000000000000
    sum_19hz = 0.00000000000000000000000

    avg_7hz = 0.00000000000000000000000
    avg_13hz = 0.00000000000000000000000
    avg_19hz = 0.00000000000000000000000

    red_flag = 0.000000000000000000000000
    blue_flag = 0.000000000000000000000000
    green_flag = 0.000000000000000000000000

    global flag_2
    global flag_3


    global m_avg_7hz
    global m_avg_13hz
    global m_avg_19hz

    # 호출시점부터 30s 간 돌아감. (Append)
    while (True):
        if (cnt_req_0 > cnt):
            sleep(0.1)
        d_2 = datetime.datetime.now()
        d_m_now = d_2.minute * 60 + d_2.second
        for i in range(-2, 3):
            lines_7hz.append(all_lines[cnt_req_0][idx_7hz + i])
            lines_13hz.append(all_lines[cnt_req_0][idx_13hz + i])
            lines_19hz.append(all_lines[cnt_req_0][idx_19hz + i])
            cnt_req_0 += 1

        if (d_m_first - d_m_now > 30):
            break

    for i in range(cnt_first, cnt_req_0 + 1):
        sum_7hz += lines_7hz[i]
        sum_13hz += lines_13hz[i]
        sum_19hz += lines_19hz[i]

    avg_7hz = sum_7hz/float(cnt_req_0+1-cnt_first)
    avg_13hz = sum_13hz/float(cnt_req_0+1-cnt_first)
    avg_19hz = sum_19hz/float(cnt_req_0+1-cnt_first)

    red_flag = avg_7hz - m_avg_7hz
    blue_flag = avg_13hz - m_avg_13hz
    green_flag = avg_19hz - m_avg_19hz

    if((red_flag >= blue_flag) and (red_flag >= green_flag)):
        flag_3=flag_2+'-'+'Red'
    elif((blue_flag >= red_flag) and (blue_flag >= green_flag)):
        flag_3=flag_2+'-'+'Blue'
    elif((green_flag >= red_flag) and (green_flag >= blue_flag)):
        flag_3=flag_2+'-'+'Green'


    return {"result": {
        'flag_3': flag_3

    }}