class Node:
    def __init__(self, v=0, nxt=None):
        self.val = v
        self.next = nxt 

def remove_from_end(head, n):
    
    first = head 
    second = head 
    c = 0

    while c<n:
        second = second.next 
        c += 1
    
    prev_node = None 

    while second!=None:
        prev_node = first
        first = first.next
        second = second.next

    next_node = first.next

    if not prev_node: # Means we're at head and need to remove head
        first.next = None 
        head = next_node 
        return head
    else:
        first.next = None
        prev_node.next = next_node 

    return head 

head = Node(5)
head.next = Node(7)
head.next.next = Node(3)
head.next.next.next = Node(2)
head.next.next.next.next = Node(4)

head = remove_from_end(head, 2)

while head!=None:
    print(head.val, end=" ")
    head = head.next

head = Node(6)
head = remove_from_end(head, 1)

head = Node(5)
head.next = Node(7)
head.next.next = Node(3)
head.next.next.next = Node(2)
head.next.next.next.next = Node(4)

head = remove_from_end(head, 1)

while head!=None:
    print(head.val, end=" ")
    head = head.next

head = remove_from_end(head, 5)

while head!=None:
    print(head.val, end=" ")
    head = head.next