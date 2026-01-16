#%%
# 맨처음 -> 반복횟수
# 지금 내놓을것 : 1<= k<=100 & 2<n<=50

for i in range(1,int(input())+1):
    
    target_list = list(map(int, input().split()))
    #앞에 몇명 받을지는 쓸대가 없다. # 문제에서 내림차순 정렬을 하라했으니 일단 하겠다.
    #다만 굳이 내림차순 안해도 되긴함.
    target_list = sorted(target_list[1:], reverse= True)
    
    #gap 구하는 최대값 저장 코드
    largest_gap = 0
    for target_index in range(1,len(target_list)):
        check_gap = target_list[target_index-1] - target_list[target_index]
        if check_gap > largest_gap:
            largest_gap = check_gap
    print(f'Class {i}')
    #내림차순 정렬 --> 이러면 max는 첫번쨰 인자값, min은 마지막 값이 될거다.
    print(f'Max {target_list[0]}, Min {target_list[-1]}, Largest gap {largest_gap}')