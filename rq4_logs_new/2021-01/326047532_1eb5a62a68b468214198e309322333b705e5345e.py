class deque:
	#멤버변수
	items = [] 
	
	#멤버함수
	def __init__(self, s): #s문자열을 받아서, 그 문자들의 리스트로 items를 초기화
		self.items = list(s) 

	def append(self, c): #c를 items리스트의 오른쪽에 append함
		self.items.append(c)

	def appendleft(self, c): #c를 item리스트의 왼쪽에 append함
		self.items.insert(0, c)

	def pop(self): #items리스트의 가장 오른쪽 원소를 삭제해서 리턴
		return self.items.pop()
	
	def popleft(self): #items리스트의 가장 왼쪽 원소를 삭제해서 리턴
		return self.items.pop(0) 
	
	def __len__(self): #리스트의 길이를 리턴
		return len(self.items) 

	def right(self): #리스트의 가장 오른쪽 원소를 삭제 안 하고 리턴
		return self.items[-1]

	def left(self): #리스트의 가장 왼쪽 원소를 삭제 안 하고 리턴
		return self.items[0] 

def check_palindrome(s):
	dq = deque(s)
	palindrome = True
	while len(dq) > 1:
		if dq.popleft() != dq.pop(): 
			palindrome = False
	return palindrome

print(check_palindrome(input()))