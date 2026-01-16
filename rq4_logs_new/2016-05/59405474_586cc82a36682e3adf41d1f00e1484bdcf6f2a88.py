import random

def select_case(filename, n = 10):
    def fn(s):    #Функция преобразует строку в список слов, '\n' и '\t'
        st = ''
        m = []
        for i in range(len(s)):
            if (s[i] == '\t' or s[i] == '\n') and i == 0:
                m.append(s[i])
            elif (s[i] == '\t' or s[i] == '\n') and i != 0:
                if st == '':
                    m.append(s[i])
                else:
                    m.append(st)
                    m.append(s[i])
                    st = ''
            elif (i == len(s)-1) and (st != ''):
                m.append(st)
            else:
                st = st + s[i]
        st = ''
        return m

    def fn1(s):     #Функция преобразует список в непрерывную строку для записи в файл
        st = ''
        for i in range(len(s)):
            st = st + s[i]
        return st

    def fn2(s,n):     #Функция выбирает список случайных номеров слов из списка слова, не рассматривая '\n' и '\t'
        d = {}
        j = []

        for i in range(len(s)):
            if s[i] != '\n' and s[i] != '\t':
                d[i] = s[i]
        j = random.sample(list(d),n)
        j.sort()
        return j

    def rename(s):
        newname = ''
        for i in range(len(s)):
            if s[i] != '.':
                newname = newname + s[i]
            else:
                newname = newname + '_res'+s[i]
        return newname

    f = open(filename)
    s = f.read()
    f.close()
    m = fn(s)
    j = fn2(m,n) #Выборка случайных номеров слов на удаление
    k = [] #Новый список слов

    for i in range(0, n):
        k.append(m.pop(j[i]-i) + '\n')
    f = open('new_pairs.txt', 'w')
    f.write(fn1(m))
    f.close()
    fres = rename(filename)
    f = open(fres, 'w')
    f.write(fn1(k))
    f.close()
    


    return fres


