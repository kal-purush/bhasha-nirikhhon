
/**
 * Definition for singly-linked list.
 * public class ListNode {
 *     int val;
 *     ListNode next;
 *     ListNode(int x) { val = x; }
 * }
 */
class Solution {
    public ListNode removeNthFromEnd_twopass(ListNode head, int n) {
        /*
        207 / 207 test cases passed.
Status: Accepted
Runtime: 14 ms
        68.62%        
        */
        int count = 0;
        ListNode curr = head;
        ListNode pre = head;
        
        if (head == null)
            return null;
        
        while (curr != null) {
            count ++;
            curr = curr.next;
        }
           
        curr = head;
        if (count == n) { //remove the first one
            head = head.next;
            return head;
        }
        while (curr != null) {
            if (count == n) {
                pre.next = curr.next;
                break;
            }
            count --;
            pre = curr;
            curr = curr.next;
        }
        
        return head;
    }
    public ListNode removeNthFromEnd(ListNode head, int n) {
    /*
    one pass
    two pointer.
    make slow, fast two pointer
    fast is move forward n+1 node
    when fast reach end, slow is the node should be removed
     1->2->3->4->5
     
     207 / 207 test cases passed.
Status: Accepted
Runtime: 15 ms
42.03%
    */
        ListNode fast = head;
        ListNode slow = new ListNode(0);
        slow.next = head;
        int count = 0;
        if (head == null) 
            return head;
        while (fast != null) {
            count ++;
            fast = fast.next;
            if (count >= (n+1)){
                slow = slow.next;
            }
        }
        
        if (slow.next == head) // remove the first node
            head = head.next;
        else
            slow.next = slow.next.next;
        
        return head;
    }

}