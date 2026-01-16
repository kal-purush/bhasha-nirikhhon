# 3w. Proszę napisać funkcję scalającą dwie posortowane listy w jedną
# posortowaną listę. Do funkcji należy przekazać wskazania na pierwsze
# elementy obu list, funkcja powinna zwrócić wskazanie do scalonej listy.
# - funkcja iteracyjna,
# - funkcja rekurencyjna.



class Node:
    def __init__(self, value=None):
        self.value = value
        self.next = None

def add_elements(t, first):
    for i in range(len(t) - 1, -1, -1):
        q = Node(t[i])
        first, q.next = q, first
    return first

def wypisz(p): #wartownika ma
    p=p.next
    while p is not None:
        print(p.value, end="")
        print(" → ", end="")
        p = p.next
    print("None")

def skaluj(first1,first2):
    p = first1.next
    q = first2.next
    s = x = Node(0)
    while p is not None or q is not None:
        while (p is None and q is not None) or (q is not None and q.value <= p.value):
            x.next = Node(q.value)
            x = x.next
            q = q.next # dodaję q.value i przesuwam q
        if p is not None:
            x.next = Node(p.value)
            x = x.next
            p = p.next # dodaję p.value i przesuwam p
    return s.next


if __name__ == '__main__':
    t1 = [0,1,2,3,4,5,6]
    t2 = [0,3,5,7,9]
    first1 = add_elements(t1,None)
    first2 = add_elements(t2,None)
    first3 = skaluj(first1,first2)
    wypisz(first1)
    wypisz(first2)
    wypisz(first3)