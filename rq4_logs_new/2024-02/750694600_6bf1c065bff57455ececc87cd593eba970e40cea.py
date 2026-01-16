N=int(input())   # 첫번째로 양의 정수가 주어진다

result=[]     # 개수의 수들 저장
cnt_list=[]   # 최대 개수 저장
for i in range (N,-1,-1):  # 주어진 수에 대한 모든 경우의 수 계산
    B=i           
    j,cnt=0,0
    
    List=[A,B]    # 첫번째와 두번째 수 저장
    while 1:
        C= List[j]-List[j+1]   # 앞앞수 - 앞수
        List.append(C)         # 구한 결과 리스트에 추가
        j+=1
        cnt+=1
        result.append(List)    # 주어진 규칙으로 만들어진 리스트들 추가
        cnt_list.append(cnt)   # 총 계산된 횟수 추가
        
        if C<0:                # 음의 정수 나오면 반복문 끝내기
            break
        

# 최대 개수의 수들을 구하는 문제
# cnt_list에서 가장 큰 수 = 최대 개수
# cnt_list에 가장 큰 수가 저장되어 있는 위치 = 
# result에 최대 개수의 수들이 저장 되어 있는 위치
result_print=result[cnt_list.index(max(cnt_list))]

# 음수까지 저장되어서 구하고자 하는 리스트에 마지막(음수)를 제거
result_print.remove(result_print[-1])

print(len(result_print))     # 최대 개수
print(*result_print)         # 최대 개수의 수들
 