# Definition for singly-linked list.
# class ListNode:
#     def __init__(self, val=0, next=None):
#         self.val = val
#         self.next = next
class Solution:
    def removeElements(self, head: ListNode, val: int) -> ListNode:

        d_head = ListNode(-1)
        d_head.next = head
        
        c_node = d_head
        while c_node.next != None:
            if c_node.next.val == val:
                c_node.next = c_node.next.next
            else:
                c_node = c_node.next
                
        return d_head.next