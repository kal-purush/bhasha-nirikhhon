# Definition for singly-linked list.
# class ListNode:
#     def __init__(self, val=0, next=None):
#         self.val = val
#         self.next = next
class Solution:
    def addTwoNumbers(self, l1: Optional[ListNode], l2: Optional[ListNode]) -> Optional[ListNode]:
        carry_over = 0
        prev_node = None
        start_node = None
        while l1 and l2:
            res = int((l1.val + l2.val + carry_over)%10)
            carry_over = int((l1.val + l2.val  + carry_over)/10)
            curr_node = ListNode(val=res)

            if prev_node:
                prev_node.next = curr_node
            else:
                start_node = curr_node
            
            prev_node = curr_node
            l1 = l1.next
            l2 = l2.next
        
        while l1:
            res = int((l1.val + carry_over)%10)
            carry_over = int((l1.val + carry_over)/10)
            curr_node = ListNode(val=res)
            prev_node.next = curr_node
            prev_node = curr_node
            l1 = l1.next
        
        while l2:
            res = int((l2.val + carry_over)%10)
            carry_over = int((l2.val + carry_over)/10)
            curr_node = ListNode(val=res)
            prev_node.next = curr_node
            prev_node = curr_node
            l2 = l2.next
        
        if carry_over:
            prev_node.next = ListNode(val=carry_over)
        
        return start_node
        