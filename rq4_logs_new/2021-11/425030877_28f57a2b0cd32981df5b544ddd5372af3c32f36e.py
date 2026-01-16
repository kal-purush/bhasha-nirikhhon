import math
import matplotlib.pyplot as plt

G = 6.67e-11  # гравитационная постоянная
M_earth = 5.97e24  # масса Земли
M_moon = 7.35e22  # масса Луны
R_moon = 1737100  # радиус Луны
T_moon = 27.32 * 24 * 3600  # сидерический период вращения Луны по орбите
r0 = 384400000  # расстояние от Земли до Луны
r1 = (2 * r0 * M_earth - (4 * r0 ** 2 * M_moon * M_earth) ** 0.5) / (2 * (M_earth - M_moon))
# расстояние от Земли до точки Лагранжа
r2 = r0 - r1  # расстояние от Луны до точки Лагранжа
v_moon = 2 * math.pi * r0 / T_moon
r = 42160416  # первоначальное расстояние до центра Земли
m = 10  # масса КА
S = 100  # площадь паруса
E = 1370  # освещенность Солнца
c = 3e8  # скорость света
x, y = 0, -r
a0_moon = (0 / T_moon) % (2 * math.pi)
x_moon, y_moon = r0 * math.sin(a0_moon), r0 * math.cos(a0_moon)
v0 = (G * M_earth / r) ** 0.5
v = (v0, math.pi / 2)
t = 0

print("Стартуем с геостационарной орбиты.")
print("Будем открывать парус, когда КА летит в направлении от Солнца.")
print("Парус будем размещать так, чтобы сила воздействия на КА была направлена"
      " в сторону его движения.")
print("Наберитесь терпения: лететь больше года.")
print()


def ang_move_moon():  # угол от оси y на Луну
    return (-2 * math.pi * t / T_moon) % (2 * math.pi)


def coords_moon():  # координаты Луны
    return r0 * math.sin(ang_move_moon()), r0 * math.cos(ang_move_moon())


def ang_earth():  # расчёт угла направления на Землю
    if y == 0 and x > 0:
        ang = math.pi * 3 / 2
    elif y == 0 and x < 0:
        ang = math.pi * 1 / 2
    elif y < 0:
        ang = (math.atan(x / y)) % (math.pi * 2)
    else:
        ang = (math.pi + math.atan(x / y)) % (math.pi * 2)
    return ang


def ang_moon():  # расчёт угла направления на Луну
    dx = abs(x - x_moon)
    dy = abs(y - y_moon)
    if dy == 0 and dx > 0:
        ang = math.pi * 3 / 2
    elif dy == 0 and dx < 0:
        ang = math.pi * 1 / 2
    elif dy < 0:
        ang = (math.atan(dx / dy)) % (math.pi * 2)
    else:
        ang = (math.pi + math.atan(dx / dy)) % (math.pi * 2)
    return ang


def r_earth():  # расстояние до Земли
    return (x ** 2 + y ** 2) ** 0.5


def r_moon():  # расстояние до Луны
    dx = x - x_moon
    dy = y - y_moon
    return (dx ** 2 + dy ** 2) ** 0.5


def f_earth():  # сила, действующая от Земли на КА
    return G * M_earth * m / r_earth() ** 2


def f_moon():  # сила, действующая от Луны на КА
    return G * M_moon * m / r_moon() ** 2


def ang_move_CA():  # угол между осью y и направлением на КА
    phi = v[1]
    if phi > math.pi:
        phi *= -1
    return ((math.pi - phi) / 2) % (2 * math.pi)


def f_sun():  # сила тяги солнечного ветра
    a = ang_move_CA()
    return 2 * S * E / c * math.sin(a)


def angle_sort(v1, v2):  # сортировка векторов в порядке уменьшения угла
    if v1[1] - v2[1] > math.pi:
        v1 = (v1[0], v1[1] - math.pi * 2)
    if v1[1] < v2[1]:
        v1, v2 = v2, v1
    return v1, v2


def v12sum(v1, v2):  # сумма двух векторов
    if v1[0] == 0:
        return v2
    elif v2[0] == 0:
        return v1

    y1 = v1[0] * math.cos(v1[1])
    y2 = v2[0] * math.cos(v2[1])
    y3 = y1 + y2

    x1 = v1[0] * math.sin(v1[1])
    x2 = v2[0] * math.sin(v2[1])
    x3 = x1 + x2

    v_val = (x3 ** 2 + y3 ** 2) ** 0.5
    v_ang = math.atan(x3 / y3)
    if y3 < 0:
        v_ang += math.pi

    return v_val, v_ang


def acc():  # расчёт ускорения под действием сил
    a1 = (f_earth() / m, ang_earth())
    a2 = (f_moon() / m, ang_moon())
    return v12sum(a1, a2)


def speed(vv, a):  # расчёт изменения скорости под влиянием ускорения (величина, угол к оси y)
    a = (a[0] * dt, a[1])

    if 0 <= vv[1] + math.pi / 2 <= math.pi and not f:
        vv = (vv[0] + f_sun() / m * dt, vv[1])
    v1, v2 = angle_sort(vv, a)
    v_val, v_ang = v12sum(v1, v2)
    return v_val, v_ang


def move():  # расчёт перемещения
    global v, y, x
    a = acc()
    v = speed(v, a)
    x += v[0] * math.sin(v[1]) * dt
    y += v[0] * math.cos(v[1]) * dt
    return x, y


def flight_graf():  # построение графиков
    fig1, ax1 = plt.subplots()
    ax1.plot(list_x, list_y)
    ax1.grid()
    ax1.set_xlabel('ось X, м')
    ax1.set_ylabel('ось Y, м')
    name = "graf.png"
    fig1.savefig(name)

    fig2, ax2 = plt.subplots()
    ax2.plot(list_t, list_h)
    ax2.grid()
    ax2.set_xlabel('время, с')
    ax2.set_ylabel('расстояние до Земли, м')
    name2 = "graf2.png"
    fig2.savefig(name2)


list_x = []
list_y = []
list_t = []
list_h = []

f = 0
dt = 10

while t < 50000000:
    t += dt
    x1, y1 = move()
    h = (x ** 2 + y ** 2) ** 0.5
    if h >= r0 and f == 0:
        vx = v[0]
        print("\nДостигнута расстояние орбиты Луны.")
        print("Расстояние до Земли =", round(h / 1000, 2), "км, Время полёта =", t, "с,",
              "скорость =", round(vx, 1), "м/с")
        print("\n")
        f = 1
    mon = 60 * 60 * 24 * 30
    if t / mon == int(t / mon):
        print("Время полёта:", int(t / mon), "мес.")
    x_moon, y_moon = coords_moon()
    list_x.append(x)
    list_y.append(y)
    list_t.append(t)
    list_h.append(h)

flight_graf()
plt.show()