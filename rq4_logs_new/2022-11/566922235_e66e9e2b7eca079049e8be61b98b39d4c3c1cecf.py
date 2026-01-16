n1=int(input("Введите число: "))
op=input("Введите операцию с этим числом(+,-,*,/): ")
n2=int(input("Введите число для операции:  "))
if op=="+":
    print(n1+n2)
elif op== "-":
    print(n1-n2)
elif op== "*":
    print(n1*n2)
elif op== "/":
    print(n1/n2)
else:
    print("Такой операции нет")