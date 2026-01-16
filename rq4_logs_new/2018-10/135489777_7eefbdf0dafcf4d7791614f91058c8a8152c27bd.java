/**
 * Definition for singly-linked list with a random pointer.
 * class RandomListNode {
 *     int label;
 *     RandomListNode next, random;
 *     RandomListNode(int x) { this.label = x; }
 * };
 */
public class Solution {
    
    private void copyList(RandomListNode head){
        RandomListNode walker = head;
        while(walker != null){
            RandomListNode node = new RandomListNode(walker.label);
            RandomListNode next = walker.next;
            walker.next = node;
            node.next = next;
            walker = next;
        }    
    }
    
    private void linkList(RandomListNode head){
        RandomListNode walker = head;
        RandomListNode runner = head.next;
        while(walker != null){
            if(walker.random != null)
                runner.random = walker.random.next;
            walker = runner.next;
            if(walker != null)
                runner = walker.next;
        }
    }
    
    private RandomListNode extractList(RandomListNode head){
        RandomListNode copyHead = new RandomListNode(0);
        RandomListNode walker = head;
        RandomListNode trace = copyHead;
        while(walker != null){
            RandomListNode next = walker.next;
            trace.next = next;
            walker.next = next.next;
            walker = walker.next;
            trace = trace.next;
        }
        return copyHead.next;
    }
    public RandomListNode copyRandomList(RandomListNode head) {
        if(head == null) return head;
        copyList(head);
        linkList(head);
        return extractList(head);
    }
}