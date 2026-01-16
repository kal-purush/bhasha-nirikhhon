# %%
n_list = list()
for trial in range(int(input())):
    if trial ==0:
        target = int(input()) #언제나 1번 후보랬으니 미리 빼놓는다.
        continue
    n_list.append(int(input()))
answer = 0
while (True):
    try:
        if target > max(n_list):
            break
    except:
        break
    #이거 try except 한 이유는 1명만 출마했을때 max(n_list)가 empty인 상황 고려한 것.
    
    #max확인하고 그에 해당하는 그 인덱스를 찾아 -1시키고, 그후 target +1
    else:
        target += 1
        n_list[n_list.index(max(n_list))] -=1
        answer += 1
print(answer)