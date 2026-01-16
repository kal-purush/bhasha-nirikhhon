# list로 구현한 stack
class Stack:

    # Constructor(생성자), 데이터를 저장할 list를 초기화
    def __init__(self):
        self.stack = []
        self.length = 0

    # list가 비어있는지 확인
    def isEmpty(self):
        return self.length==0

    # list에 데이터(elem) 추가
    def push(self, elem):
        self.stack.append(elem)
        self.length +=1

    # list에서 데이터 삭제
    def pop(self):
        if not self.isEmpty():
            self.stack.pop()
            self.length-=1

    # list에서 데이터 반환
    def peek(self):
        if not self.isEmpty():
            return self.stack[-1]
        else:
            return None

lineNum = int(input())
for i in range(lineNum):
    stack = Stack()
    key = 0
    line = input()
    for ch in line:
        if ch == '(':
            stack.push('(')
        elif ch == ')':
            if stack.peek()=='(':
                stack.pop()
            else :
                key = 1
                break
        elif ch == '[':
            stack.push('[')
        elif ch == ']':
            if stack.peek()=='[':
                stack.pop()
            else :
                key = 1
                break
        elif ch == '{':
            stack.push('{')
        elif ch == '}':
            if stack.peek() == '{':
                stack.pop()
            else:
                key=1
                break
    if stack.isEmpty() and key == 0:
        print('YES')
    else:
        print('NO')


 