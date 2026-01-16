
''' 바보같은 실수를 해서 헤맸던 문제였다.
table을 만들때 진행했는데

two_line = line.index(2)
two_row = row
<< 이 부분을 바보같이

two_row = line.index(2)
two_line = row
이렇게 적어놓고 진행했었다.

문제 그대로 의도하던 대로 짜면 되는 문제였다
'''
# %%
#테이블 생성
lines = int(input())
table = []
for row in range(lines):
    line = list(map(int, input().split()))
    if 2 in line:
        two_line = line.index(2)
        two_row = row
        
    if 5 in line:
        five_line = line.index(5)
        five_row = row
    # 생성하면서 2랑 5가 나온건 미리 저장해둠
    table.append(line)


# 거리 함수 구하기 sqrt를 굳이 안쓰고 그냥 제곱상태에서 비교 5**2 == 25이상이면 거리가 가능.
def define_distance(two_row,two_line,five_row,five_line):
    distance = (five_row-two_row)**2 + (five_line-two_line)**2
    if distance >= 25:
        return True
    else:
        return False
#2, 5 위치중 작은 값과 큰 값 정렬 (이후 행렬할때 필요)
def order_large(two_method, five_method):
    if five_method >= two_method:
        return two_method , five_method
    else:
        return five_method , two_method

def count_one(small_row, large_row, small_line, large_line):
    count_one = 0
    #2와 5 위치 를 기반으로 1의 갯수 찾는 알고리즘.(네모칸 기준)
    #같은 행/렬일때
    if small_row == large_row:
        for line_index in range(small_line, large_line+1):
            if table[small_row][line_index] ==1:
                count_one += 1
    elif small_line == large_line:
        for row_index in range(small_row,large_row+1):
            if table[row_index][small_line] ==1:
                count_one += 1
    #직사각형을 만들어야할때
    else: 
        for row_index in range(small_row, large_row+1):
            for line_index in range(small_line,large_line+1):
                if table[row_index][line_index] == 1:
                    count_one += 1
    return count_one                

#큰값 작은값 정렬.
small_row, large_row = order_large(two_row, five_row)
small_line, large_line = order_large(two_line, five_line)
#1갯수 정하는 변수
count = count_one(small_row, large_row, small_line, large_line)

#거리 조건이 맞으면 , 그다음 1의 갯수가 맞으면 1 아니면 0             
if define_distance(small_row,small_line,large_row,large_line) and (count>=3):
    print(1)
else:
    print(0)
