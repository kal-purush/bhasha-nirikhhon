T = int(input())

for i in range(1, T+1):
    # 공장을 사는데 필요한 쿠키수, 공장에서 초당 얻는 쿠키수, 목표량
    C, F, X = map(float, input().split())
    
    time = 0.0 # 총 경과 시간
    rate = 2.0 # 초당 쿠키 생산률
    
    # X / rate : 현재 속도로 목표량을 달성하는데 걸리는 시간
    # c / rate + (x / (rate + f)) : 새로운 공장을 구매한 경우의 시간과 현재 속도로 목표량을 달성하는데 걸리는 시간의 합
    while X / rate > (C / rate + (X / (rate + F))): # 현재 속도로만 생산하는 것보다 새로운 공장을 구매하여 생산 속도를 높이는 것이 더 빠른지를 판단(조건을 만족하면 새로운 공장을 계속 구매)
            time += C / rate
            rate += F
    time += X / rate

    print(f'Case #{i}: {time:.7}')