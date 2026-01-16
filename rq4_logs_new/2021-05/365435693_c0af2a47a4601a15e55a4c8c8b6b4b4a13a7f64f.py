import time
import random

while 1:
    while 1:
        print('옵션을 선택하세요 {}'.format('1P or 2P'))
        option=input()
        operation = 0
        if option == '1P':
            operation = 1
            break
        elif option == '2P':
            operation = 2
            break
        else:
            print('옵션을 다시 입력해주세요!!')
    if operation == 1:
        countup=0
        countdown=15
        number=random.randrange(1,1001)
        print('모드를 선택하십시오 {}'.format('카운트다운 or 무제한'))
        answer2=input()
        if answer2=='카운트다운':
            while 1:
                print("숫자를 추리하세요")
                answer=int(input())
                if answer<1 or answer>1000:
                    print('범위를 벗어난 숫자를 입력하였습니다. 다시 입력하세요')
                    continue
                else:
                    if answer==number:
                        print('정답!')
                        print('답은 {}이었습니다'.format(number))
                        break
                    elif answer <number:
                        countdown-=1
                        print('업!')
                        print('앞으로 {}남았습니다'.format(countdown))
                        continue
                    elif answer > number:
                        countdown -= 1
                        print('다운!')
                        print('앞으로 {}남았습니다'.format(countdown))
                        continue
                    elif countdown ==0:
                         print('game over')
                         break
        elif answer2 == '무제한':
            while 1:
                print("숫자를 추리하세요")
                answer=int(input())
                if answer<1 or answer>1000:
                    print('범위를 벗어난 숫자를 입력하였습니다. 다시 입력하세요')
                    continue
                else:
                    if answer==number:
                        print('정답!')
                        print('답은 {}이었습니다'.format(number))
                        print('{}번째 시도만에 성공하였습니다'.format(countup))
                        break
                    elif answer <number:
                        countup+=1
                        print('업!')
                        print('현재 {}번째 시도입니다.'.format(countup))
                        continue
                    elif answer > number:
                        countup += 1
                        print('다운!')
                        print('현재 {}번째 시도입니다.'.format(countup))
                        continue
        print(countup)
        print('이름을 입력하시오')
        name=input()
        time = time.strftime('%Y-%m-%d', time.localtime(time.time()))
        str2 = str(countup) + "_"+time+"_"+name
        print('{}'.format(str2))
    if operation == 2:
        change=1
        number = random.randrange(1, 1001)
        while 1:
            if change==1:
                print('1PLAYER')
                print("숫자를 추리하세요")
                answer = int(input())
                if answer < 1 or answer > 1000:
                    print('범위를 벗어난 숫자를 입력하였습니다. 다시 입력하세요')
                    continue
                else:
                    if answer == number:
                        print('답은 {}이었습니다'.format(number))
                        print('The winner is player 2! 축하합니다!"')
                        break
                    elif answer < number:
                        change = 2
                        print('업! 다음 플레이어에게 턴이 넘어갑니다.')
                        continue
                    elif answer > number:
                        change = 2
                        print('다운! 다음 플레이어에게 턴이 넘어갑니다.')
                        continue
            if change==2:
                print('2PLAYER')
                print("숫자를 추리하세요")
                answer = int(input())
                if answer < 1 or answer > 1000:
                    print('범위를 벗어난 숫자를 입력하였습니다. 다시 입력하세요')
                    continue
                else:
                    if answer == number:
                        print('답은 {}이었습니다'.format(number))
                        print('The winner is player 2! 축하합니다!"')
                        break
                    elif answer < number:
                        change = 1
                        print('업! 다음 플레이어에게 턴이 넘어갑니다."')
                        continue
                    elif answer > number:
                        change = 1
                        print('다운! 다음 플레이어에게 턴이 넘어갑니다.')
                        continue