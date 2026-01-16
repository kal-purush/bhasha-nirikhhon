Recursive solution


/**
 * Definition for singly-linked list.
 * public class ListNode {
 *     int val;
 *     ListNode next;
 *     ListNode(int x) { val = x; }
 * }
 */
class Solution {
    public ListNode swapPairs(ListNode head) {
       if( head ==null || head.next == null )
           return head;
       else{
        ListNode remList = head.next.next;
        int temp = head.val;
        head.val = head.next.val;
        head.next.val = temp;
        remList = swapPairs(remList);
        head.next.next = remList;
        
        return head;
        }  
    }
}

***************************************************
Iterative solution

/**
 * Definition for singly-linked list.
 * public class ListNode {
 *     int val;
 *     ListNode next;
 *     ListNode(int x) { val = x; }
 * }
 */
class Solution {
    public ListNode swapPairs(ListNode head) {
       if( head ==null || head.next == null )
           return head;
       ListNode temp = head; 
  
        /* Traverse only till there are atleast 2 nodes left */
        while (temp != null && temp.next != null) { 
  
            /* Swap the data */
            int k = temp.val; 
            temp.val = temp.next.val; 
            temp.next.val = k; 
            temp = temp.next.next; 
         } 
        
      return head;
    }
}