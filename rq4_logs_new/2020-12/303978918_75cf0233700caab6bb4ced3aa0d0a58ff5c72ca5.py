import math
#Moc dau cua x
x0=0
#Gia tri x tai moc dau
y0=1
#moc cuoi cua doan can tinh
xn=1
h=0.1
yn=1
n=int((xn-x0)/h)
RKE=[y0]
yad=0


#Ham f
def f(x,y):
    return y-x**2


#Tinh k
def k(x,y):
	return (h*f(x,y))

#So do RK4
def RK4(x0,y0,xn,h):
    for i in range(n):
        k1 = k(x0,y0)
        k2 = k(x0+(0.5)*h,y0+(0.5)*k1)
        k3 = k(x0+(0.5)*h,y0+(0.5)*k2)
        k4 = k(x0+h,y0+k3)
        yn = y0+(k1+2*k2+2*k3+k4)/6
        RKE.append(yn)
        y0 = yn
        x0 = x0+h
    return RKE
#Adam noi
def adams(h,RKE,yad):

	for i in range(3,n):
		yad = RKE[i] + (h/720)*(251*f((i+1)*h,RKE[i+1])+646*f(i*h,RKE[i])-264*f((i-1)*h,RKE[i-1])+106*f((i-2)*h,RKE[i-2])-19*f((i-3)*h,RKE[i-3]))
		print('%.0f\t%.1f\t%.9f\t%.9f'% (i+1,(i+1)*h,yad,RKE[i+1]))

#Tinh toan RK4
RK4(x0,y0,xn,h)

#In ket qua Adams
print('\n--------BANG GIA TRI--------')
print('-------------------------')    
print('i\txi\tAdamsnoi\t\tRK4')
print('-------------------------')
print(adams(h,RKE,yad))





