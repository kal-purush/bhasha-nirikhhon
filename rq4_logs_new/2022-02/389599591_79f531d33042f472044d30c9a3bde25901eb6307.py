import sys
import math


n = int(input())  # the number of temperatures to analyse
if n==0:
    print(0)
else:
    result=5527
    for i in input().split():
        # t: a temperature expressed as an integer ranging from -273 to 5526
        t = int(i)
        if abs(t)<abs(result):
            result=t
        elif abs(t)==abs(result) and t > result: #cas d'égalité on prend la val postif
            result=t
        else:
            pass

    print(result)