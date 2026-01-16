/**
 * Definition for singly-linked list.
 * public class ListNode {
 *     int val;
 *     ListNode next;
 *     ListNode(int x) { val = x; }
 * }
 */
public class Solution {
    public ListNode reverseKGroup(ListNode head, int k) {
        if(head == null){
            return null;
        }
        // we need to change the head, so here dummy is used 
        ListNode dummy = new ListNode(0);
        dummy.next = head;
        int count = 0;
        ListNode pre = dummy;
        ListNode cur = head;
        
        while(cur != null){
            count++;
            ListNode next = cur.next;
            if(count == k){
                pre = reverse(pre, next);
                count = 0;
            }
            // continue visit next node
            cur = next;
        }
        
        return dummy.next;
    }
    
    public ListNode reverse(ListNode pre, ListNode end){
        ListNode last = pre.next;
        ListNode cur = last.next;
        
        while(cur != end){
            last.next = cur.next;
            cur.next = pre.next;
            pre.next = cur;
            cur = last.next;
        }
        
        return last;
    }
    
}