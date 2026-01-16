package my.com.app;

import java.util.*;
/**
 * Definition for singly-linked list.
 * public class ListNode {
 *     int val;
 *     ListNode next;
 *     ListNode(int x) { val = x; }
 * }
 */

class Solution {
    
    public ListNode mergeKLists(ListNode[] lists) {
     
        if(lists.length==0)
            return null;
        
        if(lists.length==1)
            return lists[0];
        
        PriorityQueue<ListNode> que = new PriorityQueue(lists.length, new Comparator<ListNode>(){
                
            public int compare(ListNode h1, ListNode h2){
                
                return (h1.val<=h2.val)?-1: 1;
            }
        });
    
        
        for(int i=0; i<lists.length; i++){
            if(lists[i]!=null){
                que.add(lists[i]);    
            }
        }
        
        ListNode head=null , p=null;
        
        while(que.peek() != null){
        
            ListNode x = que.poll();
            
            if(head == null){

                head = x;
                p=head;
            }
            else{
                p.next =x;
                p = p.next;
                
            }
            
                if(x.next!=null){
                    que.add(x.next);        
                }
        }
        
    return head;
    }
}