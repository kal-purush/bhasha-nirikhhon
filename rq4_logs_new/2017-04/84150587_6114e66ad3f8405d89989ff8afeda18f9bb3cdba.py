# Definition for singly-linked list.
class ListNode(object):
    def __init__(self, x):
        self.val = x
        self.next = None

class Solution(object):
    def addTwoNumbers(self, l1, l2):
        """
        :type l1: ListNode
        :type l2: ListNode
        :rtype: ListNode
        """
        l2_copy = l2
        l1_copy = l1
        l1list = []
        l2list = []
        while l1 is not None:
            l1list.append(l1.val)
            l1 = l1.next
        while l2 is not None:
            l2list.append(l2.val)
            l2 = l2.next
        index1 = len(l1list) - 1
        index2 = len(l2list) - 1
        carry = False
        head = None
        while index1 >= 0 and index2 >= 0:
            val = l1list[index1] + l2list[index2] + (1 if carry else 0)
            carry = True if val >= 10 else False
            tmp = ListNode((val % 10))
            tmp.next = head
            head = tmp
            index1 -= 1
            index2 -= 1

        while index2 >= 0:
            val = l2list[index2] + (1 if carry else 0)
            carry = True if val >= 10 else False
            tmp = ListNode((val % 10))
            tmp.next = head
            head = tmp
            index2 -= 1

        while index1 >= 0:
            val = l1list[index1] + (1 if carry else 0)
            carry = True if val >= 10 else False
            tmp = ListNode((val % 10))
            tmp.next = head
            head = tmp
            index1 -= 1

        if carry:
            tmp = ListNode(1)
            tmp.next = head
            head = tmp
        return head

def makeLinkList(list0):
    list0.reverse()
    head = None
    for i in list0:
        tmp = ListNode(i)
        tmp.next = head
        head = tmp
    return head

def showList(head):
    tmp = head
    outlist = []
    while tmp is not None:
        outlist.append(str(tmp.val))
        tmp = tmp.next
    print("->".join(outlist))

if __name__ == "__main__":

    s = Solution()
    a = makeLinkList([5,])
    b = makeLinkList([5,])
    showList(a)
    showList(b)

    c = s.addTwoNumbers(a, b)
    showList(c)