from random import choice

'''Нехай ми маємо перестановку pi, яку можна відобразити у вигляді добутку
незалежних циклів.
Всі цикли довжини 5 або менше ми можемо одразу записати в результат, бо вони
не рухают жодного елемента при добутку.
Інші елементи ми повинні розбити на менші цикли, тому хоча б 1 елемент будет
спільним. Щоб отримати мінімальну кількість циклів тільки один елемент може
бути спільним.
n<k<m, (n ... m) - деякий цикл з добутку
=> Ми можемо розкласти його у вигляді (n ... k)(k+1 ... m)
Щоб кожеш цикл рухав не більше 5 елементів він повинен буди довжини не більше 5.
=> k <= n+4
Звідси, ми маємо розкласти кожен з циклів довжини >5 у добуток циклів довжини 5,
поки не отримаемо в кінці цикл довжини <=5.
Всі цикли довжини 5 ми відправляемо до результату. Залишилися остатки від розиття
(цикли довжини <5).
Але вони не мают спільних елементів, бо походять від різних незалежних циклів
(від неперетинаючих класів). Звідси, при добудтку один на інший ми не
отримуємо нові цикли більшої довжини. Тому ми можемо відправити усі ці цикли до
результату. Більше циклів не залишилося.
'''

class PermutationOfCyclics():
    '''Вивід списку циклів у вигляді (1 ... n)(m ... l)'''

    def __init__(self, result):
        self.result = result

    def __str__(self):
        Str = ""
        for cyc in self.result:
            Str += "("
            for el in cyc:
                Str += str(el) + " "
            Str += ")"
        return Str

def Cteate_Permutation(Length):
    '''Створюємо таблицю перестановки'''
    permutation = []
    List = list(range(1, Length+1))

    for i in range(Length):
        permutation += [(List.pop(List.index(choice(List))))]
    return permutation

def PermutationToCycle(permutation):
    '''Розбиваємо перестановку у добуток незалежних циклів'''
    mul = []
    List = list(range(1,len(permutation)+1))

    while List:
        cycle = [List.pop(0)]

        while True:
            next_element = permutation[cycle[-1]-1]
            if next_element!=cycle[0]:
                cycle += [next_element]
                List.pop(List.index(next_element))
            else:
                if len(cycle)!=1:
                    mul += [cycle]
                break
    return mul


Result = []
Length = 10000

#Створюємо перестановку
Permutation = Cteate_Permutation(Length)
PermutationMul = PermutationToCycle(Permutation)

for cycle in PermutationMul:
    '''Всі цикли довжини <=5 відправляємо до результату'''
    if len(cycle) <= 5:
        Result += [cycle]
        PermutationMul.pop(PermutationMul.index(cycle))

for i in PermutationMul:
    '''Розбиваємо усі інші цикли і відпарвляємо до результату'''
    while len(i)>5:
        Result += [i[:5]]
        i = [i[c] for c in range(len(i)) if c not in range(1,5)]
    if len(i)<=5:
        Result += [i]

print(PermutationOfCyclics(Result))
#print(Permutation)
#print(Result)