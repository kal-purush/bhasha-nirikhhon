class Node:
    def __init__(self, v=0, nxt=None):
        self.val = v
        self.next = nxt 

def add_two_linked_lists(a, b):

    c = Node() # Stores the sum
    c_head = c
    carry = 0

    while a!=None and b!=None:

        c.next = Node()
        c = c.next
        csum = a.val + b.val + carry
        carry = csum//10
        csum = csum%10 
        c.val = csum 
        a = a.next
        b = b.next
    
    if a!=None:
        while a!=None:
            c.next = Node()
            c = c.next
            csum = a.val + carry 
            carry = csum//10
            csum = csum%10 
            c.val = csum 
            a = a.next
    elif b!=None:
        while b!=None:
            c.next = Node()
            c = c.next
            csum = b.val + carry 
            carry = csum//10
            csum = csum%10 
            c.val = csum 
            b = b.next

    if a==None and b==None: #We have reached end of both
        if carry == 0:
            return c_head.next
        else: 
            c.next = Node(carry)
            return c_head.next


a = Node(5)
a.next = Node(7)
a.next.next = Node(3)

b = Node(8)
b.next = Node(4)
b.next.next = Node(4)

c = add_two_linked_lists(a,b)

while c!=None:
    print(c.val, end=" ")
    c = c.next


a = Node(0)
b = Node(0)
c = add_two_linked_lists(a,b)

while c!=None:
    print(c.val, end=" ")
    c = c.next


a = Node(1)
b = Node(9)
b.next = Node(9)
c = add_two_linked_lists(a,b)

while c!=None:
    print(c.val, end=" ")
    c = c.next

a = Node(9)
a.next = Node(9)
a.next.next = Node(9)
a.next.next.next = Node(9)
a.next.next.next.next = Node(9)
a.next.next.next.next.next = Node(9)
a.next.next.next.next.next.next = Node(9)

b = Node(9)
b.next = Node(9)
b.next.next= Node(9)
b.next.next.next = Node(9)

c = add_two_linked_lists(a,b)

while c!=None:
    print(c.val, end=" ")
    c = c.next