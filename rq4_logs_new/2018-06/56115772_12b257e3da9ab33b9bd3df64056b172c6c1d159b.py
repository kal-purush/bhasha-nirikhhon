def mergeKLists(self, lists):
    """
    :type lists: List[ListNode]
    :rtype: ListNode
    """
    # = len(lists) # Get the number of lists
    if len(lists) == 0:
        return None
    while len(lists) > 1: 
        nextList = []
        for i in range(0, len(lists) - 1, 2):
            nextList.append(self.mergeTwoLists(lists[i], lists[i+1]))
        if  len(lists) % 2 == 1:
            nextList.append(lists[len(lists) - 1])
        lists = nextList
    return lists[0]

def mergeTwoLists(self, l1, l2):
    """
    :type l1: ListNode
    :type l2: ListNode
    :rtype: ListNode
    """
    if l1 == None:
        return l2
    if l2 == None:
        return l1
    dummy = ListNode(0)
    tmp = dummy 
    
    while l1 != None and l2 != None: 
        if l1.val <= l2.val:
            tmp.next = l1
            l1 = l1.next
        else:
            tmp.next = l2
            l2 = l2.next
        tmp = tmp.next
        
    if l2 == None:
        tmp.next = l1
    else: 
        tmp.next = l2
    return dummy.next
        