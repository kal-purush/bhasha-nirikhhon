import numpy as np
import readOp

url=r"C:\Users\ENNIE\OneDrive\CIS\gsod_2019\010010-99999-2019.op"
texts=readOp.readOpcal(url)

X=[]
y=[]

#试图使用7天之前的数据来预测平均温度
for i in range(len(texts)):
    X_tmp = []
    if i+7<len(texts) and texts[i+7]:
        y.append(texts[i+7][0])
        for j in range(i,i+7):
            X_tmp.append(texts[j])
        X.append(X_tmp)

# print(X)
# print(y)

for i in X:
    print(i)

for i in y:
    print(i)

print(len(y))
print(len(texts))

print(";;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;")
for i in texts:
    print(i)