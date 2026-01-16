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
class Solution {
    public ListNode mergeTwoLists(ListNode l1, ListNode l2) {
        ListNode l3 = new ListNode(0);
        ListNode head = l3;

        while(l1 != null && l2 != null){

            if(l1.val <l2.val){
                l3.next = new ListNode(l1.val);
                l1 = l1.next;
                l3 = l3.next;
            }
            else {
                l3.next = new ListNode(l2.val);
                l2 = l2.next;
                l3 = l3.next;
            }
        }

        while(l1 !=null) {
            l3.next = new ListNode(l1.val);
            l1 = l1.next;
            l3 = l3.next;
        }

        while( l2 !=null) {
            l3.next = new ListNode(l2.val);
            l2 = l2.next;
            l3 = l3.next;
        }
        return head.next;
    }
}

// TC : O(max(l1,l2))
// sc: O(max(l1,l2))
// logic: use same logic that we use in merge sort to combine sorted elements