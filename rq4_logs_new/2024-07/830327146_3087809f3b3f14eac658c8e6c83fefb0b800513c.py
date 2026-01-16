 #while 문을 사용하여 1부터 45 사이에 중복이 없는 랜덤한 수 6개를 생성하고,
 # 이를 result 리스트 변수에 추가하는 코드를 작성하라

import random

n = 0
result = []
while n < 6:
    n = n + 1
    nn = result.append(random.randint(1, 45))
    if nn in result:
        result.remove(nn)
    else:
        result.append(nn)
print(result)