X,Y=map(int,input().split())   #가로,세로 길이 입력
x_cut=[]    # 가로 점선을 따라 자르는 경우
y_cut=[]    # 세로 점선을 따라 자르는 경우
width=[]
for _ in range (int(input())):
  a,b=map(int,input().split()) # a:가로/세로 정보, b:자르는 위치
  if a==0:
    x_cut.append(b)
  elif a==1:
    y_cut.append(b)

#테두리도 고려해야하기때문에 가장 마지막 위치 넣기    
x_cut.append(Y)
y_cut.append(X)    

x_cut.sort()
y_cut.sort()


len_x=len(x_cut)
len_y=len(y_cut)

# [2,3,8]위치에서 자르면 하면 각 길이는 [2,1,5]로 됨
for i in range (len_x-1,0,-1):
  x_cut[i]=x_cut[i]-x_cut[i-1]

# [4,10]위치에서 자르면 하면 각 길이는 [4,6]로 됨
for i in range (len_y-1,0,-1):
  y_cut[i]=y_cut[i]-y_cut[i-1]

# 각 가로와 세로끼리 곱해 모든 사각형의 넓이를 구함
for x in x_cut:
  for y in y_cut:
    width.append(x*y)

# 가장 큰 값 출력
print(max(width))