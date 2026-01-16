#https://leetcode.com/problems/add-two-numbers/
#You are given two non-empty linked lists representing two non-negative integers. The digits are stored in reverse order and each of their nodes contain a single digit. Add the two numbers and return it as a linked list.
#You may assume the two numbers do not contain any leading zero, except the number 0 itself.
#Example:
#Input: (2 -> 4 -> 3) + (5 -> 6 -> 4)
#Output: 7 -> 0 -> 8
#Explanation: 342 + 465 = 807.

 #Definition for singly-linked list.


class ListNode:
 def __init__(self, x):
     self.val = x
     self.next = None


class Solution:
    def addTwoNumbers(self, l1: ListNode, l2: ListNode) -> ListNode:
        _result = node = ListNode(0)
        carry = 0
        while l1 or l2:
            v1 = l1.val if l1 else 0
            v2 = l2.val if l2 else 0
            node.next = ListNode((v1 + v2 + carry) % 10)
            node = node.next
            carry = (v1 + v2 + carry) // 10
            l1 = l1.next if l1 else None
            l2 = l2.next if l2 else None
        if carry > 0:
            node.next = ListNode(carry)
        return _result.next

    def printlist(self, l: ListNode):
        li = []
        while l:
            val = l.val
            l = l.next
            li.append(val)
        return li


sol = Solution()
_list = l1 = ListNode(2)
l1.next = ListNode(4)
l1 = l1.next
l1.next = ListNode(3)


_list2 = l2 = ListNode(5)
l2.next = ListNode(6)
l2 = l2.next
l2.next = ListNode(4)

result = sol.addTwoNumbers(_list, _list2)
print(sol.printlist(_list2), sol.printlist(_list))
print(sol.printlist(result))


#Finished
#Runtime: 48 ms
#Your input
#[2,4,3]
#[5,6,4]
#Output
#[7,0,8]
#Expected
#[7,0,8]





