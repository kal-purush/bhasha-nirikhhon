import math
def f(x):
    #return cos(radians(90*x))
    #return cos(x)-x
    return x*x

def tabl(xn, xk, num):
    XY = []
    step = (xk - xn) / (num - 1)
    xt = xn
    while(xt <= xk):
        XY.append([xt, f(xt)])
        xt += step
    return XY


def print_t(XY, kazd):
    print("   x   ", " y   ")
    for i in range(len(XY)):
        if(i % kazd == 0):
            print("{:5.3f}".format(XY[i][0]), "{:5.3f}".format(XY[i][1]))
      

def razd_razn(XY, n, i_start):
    RR = []
    T = []
    for i in range(n):
        T.append(XY[i_start + i][0])
    RR.append(T)
    T = []
    for i in range(n):
        T.append(XY[i_start + i][1])
    RR.append(T)
    T = []
    for i in range(n - 1):
        T = []
        for j in range(n - i- 1):
            T.append((RR[i + 1][j] - RR[i + 1][j + 1]) / (RR[0][j] - RR[0][i + j + 1]))
        RR.append(T)
    return RR

            
def interpolate(RR, n, x):
    p = RR[1][0];
    for i in range(1, n):
        tek = 1
        for j in range(i):
            tek = tek * (x - RR[0][j])
        p = p + tek * RR[i + 1][0];
    return p;


def opr(XY, x, n):
    for i in range(len(XY)):
        if x == XY[i][0]:
            it = i + 1
            break
    
        if x < XY[i][0]:
            it = i
            break

    r_range = round(n / 2)
    l_range = n - r_range
    
    if it - l_range < 0:
        i_start = 0
    elif it + r_range > len(XY):
        i_start = len(XY) - n
    else:
        i_start = it - l_range
    return i_start


def print_t2(XY):
    print(XY)
    for i in range(len(XY)):
        for j in range(len(XY)):
            if j < len(XY[i]):
                print(XY[i][j])


def change(XY):
    for i in range(len(XY)):
        XY[i][0], XY[i][1] = XY[i][1], XY[i][0]
        
xn = -5
xk = 5
num = 11

XY = tabl(xn, xk, num)

print_t(XY, 1)
t = 1
while(t == 1):
    n = int(input("Введите степень полинома: "))
    n = n + 1;
    if (n < 0):
        print("Степень полинома не может быть меньшей нуля")
    elif (n > num):
       print("Степень полинома слишком высока")
    else:
        t = 0

x = float(input("Введите х: "))
if(x < xn or x > xk):
    print("Х не входит в область определения таблицы")
else:
    i_start = opr(XY, x,n)
    RR = razd_razn(XY, n, i_start)

    p = interpolate(RR, n, x)
    print("Взяты индексы с ", i_start, " по ", i_start + n - 1)
    print("Вычисленное значение f(x): {:6.4f}".format(p))
    print("Точное значение f(x): {:6.4f}".format(f(x)))
    print("Погрешность интерполяции: {:8.6f}".format(abs(f(x) - p)))