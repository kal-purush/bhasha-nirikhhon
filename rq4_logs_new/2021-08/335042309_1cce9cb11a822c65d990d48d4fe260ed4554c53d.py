class ListNode:
    def __init__(self, val=0, next=None):
        self.val = val
        self.next = next

def merge(l1, l2):
    if l1==None:
        return l2 
    elif l2==None:
        return l1
    elif l1==None and l2==None:
        return None 

    l3 = ListNode()
    head = l3

    while l1!=None and l2!=None:
        l3.next = ListNode()
        l3 = l3.next
        if l1.val<=l2.val:
            l3.val = l1.val
            l1 = l1.next
        else:
            l3.val = l2.val 
            l2 = l2.next

    while l1!=None:
        l3.next = ListNode()
        l3 = l3.next
        l3.val = l1.val 
        l1 = l1.next
    
    while l2!=None:
        l3.next = ListNode()
        l3 = l3.next
        l3.val = l2.val
        l2 = l2.next
    
    return head.next