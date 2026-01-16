/*
21. Merge Two Sorted Lists
Easy
17.8K
1.6K
Companies
You are given the heads of two sorted linked lists list1 and list2.

Merge the two lists in a one sorted list. The list should be made by splicing together the nodes of the first two lists.

Return the head of the merged linked list.



Example 1:


Input: list1 = [1,2,4], list2 = [1,3,4]
Output: [1,1,2,3,4,4]
Example 2:

Input: list1 = [], list2 = []
Output: []
Example 3:

Input: list1 = [], list2 = [0]
Output: [0]


Constraints:

The number of nodes in both lists is in the range [0, 50].
-100 <= Node.val <= 100
Both list1 and list2 are sorted in non-decreasing order.
 */

public class MergeTwoSortedLists {
    /**
     * Definition for singly-linked list.
     * public class ListNode {
     *     int val;
     *     ListNode next;
     *     ListNode() {}
     *     ListNode(int val) { this.val = val; }
     *     ListNode(int val, ListNode next) { this.val = val; this.next = next; }
     * }
     */
        public ListNode mergeTwoLists(ListNode list1, ListNode list2) {
            if(list1 == null && list2 == null)
                return null;

            if(list1 == null && list2 != null)
                return list2;

            if(list2 == null && list1 != null)
                return list1;

            var current1 = list1;
            var current2 = list2;
            ListNode previous;
            ListNode other;
            if(list1.val <= list2.val){
                previous = list1;
                other = list2;
            }
            else{
                previous = list2;
                other = list1;
            }

            var current = previous.next;

            while(current != null) {
                if(current.val > other.val) {
                    previous.next = other;
                    previous = other;
                    other = current;
                    current = previous.next;
                }
                else {
                    previous = previous.next;
                    current = current.next;
                }
            }

            while(other != null) {
                previous.next = other;
                previous = other;
                other = other.next;
            }

            return list1;
        }
}