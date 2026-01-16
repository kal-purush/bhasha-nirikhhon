from collections import deque


def get_array(str):
    if str == '[]':
        return []
    else:
        str = str.strip('[]')
        str = str.split(',')
        return str


def get_answer(deq):
    ans = '['
    ans += ','.join(deq)
    ans += ']\n'
    return ans


if __name__ == '__main__':
    answer = ""

    T = int(input())
    for i in range(T):
        p = input()
        n = int(input())
        x = input()
        deq = deque(get_array(x))
        mod = True  # front
        isRight = True

        for ch in p:
            if ch == 'R':
                mod = not mod
            elif ch == 'D':
                if len(deq) == 0:
                    answer += 'error\n'
                    isRight = False
                    break
                else:
                    if mod:
                        deq.popleft()
                    else:
                        deq.pop()
            else:
                print("ERROR")

        if isRight:
            if mod:
                answer += get_answer(deq)
            else:
                deq.reverse()
                answer += get_answer(deq)

    print(answer)