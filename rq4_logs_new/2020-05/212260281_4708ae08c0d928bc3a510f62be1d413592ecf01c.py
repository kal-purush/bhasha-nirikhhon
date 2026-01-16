# Given a non-empty, singly linked list with head node head, return a middle node of linked list.
# If there are two middle nodes, return the second middle node.

# EXAMPLE
# Input: [1,2,3,4,5]
# Output: Node 3 from this list (Serialization: [3,4,5])
# The returned node has value 3.  (The judge's serialization of this node is [3,4,5]).
# Note that we returned a ListNode object ans, such that:
# ans.val = 3, ans.next.val = 4, ans.next.next.val = 5, and ans.next.next.next = NULL.


# Definition for singly-linked list.
# class ListNode:
#     def __init__(self, x):
#         self.val = x
#         self.next = None

class Solution:
    def middleNode(self, head: ListNode) -> ListNode:
        '''
        Time: O(N), because we only make one full pass of the list
        Space: O(1), no auxillary memory is used only ptrs
        '''
        # 2 pointer approach
        slow = fast = head

        # while loop checks to make sure there are at least
        # 2 nodes in the linked list
        while fast and fast.next:
            # slow ptr moves at half speed
            slow = slow.next
            # fast ptr moves at full speed
            fast = fast.next.next

        return slow