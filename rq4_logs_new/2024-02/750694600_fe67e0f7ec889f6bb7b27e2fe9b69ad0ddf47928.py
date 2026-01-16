s=list(input())   #암호키
Key={}

for _ in range (len(s)):
  Key[_]=list(s[_])  #순서 기억을 위해 암호키와 숫자를 같이 입력

# 암호키 정렬
sorted_key=sorted(Key.items(),key=lambda x:x[1])
sorted_key=[list(item) for item in sorted_key]

# 정렬된 암호키에 암호문 넣기
Code=list(input())
start=0
col_len=len(Code)//len(s)
for i in range (start,len(s)):
  sorted_key[i].append(Code[start:start+col_len])
  start+=col_len

# 원래 암호키 순서로 돌아가기(암호문도 평문일때 순서로 돌아감)
sorted_code=sorted(sorted_key,key=lambda x:x[0])

# 암호문 평문으로 바꾸기
Text=[[] for i in range (col_len)]
for i in range (col_len):
  for j in range (len(s)):
    Text[i].append(sorted_code[j][2][i])

# print(Text)
for i in Text:
  print(''.join(i),end='')