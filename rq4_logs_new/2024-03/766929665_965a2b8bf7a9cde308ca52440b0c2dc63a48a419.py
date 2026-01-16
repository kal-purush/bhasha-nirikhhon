#%%
#1. 농장의 수 즉 farm의 갯수가 늘어나는게 효력있을때까지 돌린다가 핵심포인트.
#2. 농장이 늘어날때마다 늘어나는 시간 (C/2 + C/(2+F) + C/(2+F*2) + ... C/(2+F*N))  == H라두겠다. // N = 농장수
#    최종 시간 : H + [(2+F*N)*TIME_2 >= X]일때 TIME_2  , H + X/(2+F*N) 
#3. (2+F*N) == rate로 하고 rate를 갱신하면 편하겠다라는 게 나옴.
#    즉 sum(c/rate_n) + x/(rate_last)  #rate_n = (2+f*n) 이 됨.
#4. 이렇게 접근했을때 효과가 있는걸 증명하려면 (x-c)/rate > x/(rate_next) 가 됨.
#    즉 c를 사는게 시간을 단축하는데 이득이 될때까지 반복.
#나머지는 이를 구현하기만 하면 된다. 문제에서 소수 7자리라고 표현하면 좋겠다고 했으니 round(,7)로 표현.
trial = int(input())

for i in range(1, trial+1):
    c, f, x = map(float, input().split())
    time = 0
    rate = 2
    while x > c + rate * x/(rate + f):
        time += c/rate
        rate += f
    time += x/rate
    print(f'Case #{i}: {round(time, 7)}')
