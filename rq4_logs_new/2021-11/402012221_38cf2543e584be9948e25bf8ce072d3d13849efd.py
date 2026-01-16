# 1단계
# 파손된 램

user_input = input()
user_input2 = input()
arr = user_input2.split(' ')
error = []
for a in range(len(arr)):
	ram = int(arr[a])
	if (ram & (ram - 1)) != 0:
		error.append(a + 1)
if len(error) > 0:
	print(len(error))
	for a in error:
		print(a, end=' ')
else:
	print(0)