num=[3,10,-4,7,6,-4,1,-2,0,10,3,7] #題目串列
anw=[]  #先給一個空的串列
for n,i in enumerate(num): #迴圈
  if n>=2 and n<=9: #從第3個數字取到第10個數字
    anw.append(i) #取數字
anw.reverse() #將取的數字反轉
print(anw[0:len(anw)])  #列印最後的答案