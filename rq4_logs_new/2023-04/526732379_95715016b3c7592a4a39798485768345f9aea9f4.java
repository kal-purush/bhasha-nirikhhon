/*Given the head of a linked list, remove the nth node from the end of the list and return its head.



Example 1:


Input: head = [1,2,3,4,5], n = 2
Output: [1,2,3,5]
Example 2:

Input: head = [1], n = 1
Output: []
Example 3:

Input: head = [1,2], n = 1
Output: [1]


Constraints:

The number of nodes in the list is sz.
1 <= sz <= 30
0 <= Node.val <= 100
1 <= n <= sz

 */

public class LinkedListNthNode {
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
    public ListNode removeNthFromEnd(ListNode head, int n) {
        var current = head;
        var nthNode = head;
        var previous = head;
        int size = 0;

        if(current == null)
            return head;

        int i = 1;
        while(i<=n && current != null){
            current = current.next;
            i++;
            size++;
        }

        int check = 0;
        while(current != null) {
            size++;
            current = current.next;
            nthNode = nthNode.next;
            if(check == 1){
                previous = previous.next;
            }
            check = 1;
        }

        if(n < size)
            previous.next = nthNode.next;
        else
            head = nthNode.next;

        return head;
    }
}