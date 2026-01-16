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
class ReverseLinkedList {
    public ListNode reverseList(ListNode head) {
        if(head == null)
            return null;

        ListNode curr = null;
        ListNode prev = null;

        while(head !=null){
            curr = head;
            head = head.next;
            curr.next = prev;
            prev =curr;
        }

        return curr;
    }
}

//time complexity: O(n)
// space complexity : O(1)
// simple logic.
//1. assign each head node to curr.
//2. move head node to next;
//3. curr.next -> prev;
// 4. prev = curr.