package my.com.app;

import java.util.*;

/**
 * Definition for singly-linked list.
 * public class ListNode {
 *     int val;
 *     ListNode next;
 *     ListNode(int x) { val = x; }
 * }
 * LeetCode Problem: https://leetcode.com/problems/merge-k-sorted-lists/
 * 
 * Approach: Use a min heap to store the head of each linked list. Poll the smallest element from the heap and add it to the merged list. Add the next element from the same list to the heap. Repeat until all lists are merged.
 * 
 * Time Complexity: O(N log k) where N is the total number of nodes across all lists and k is the number of lists.
 * Space Complexity: O(k) for the priority queue.
 */



class Solution {
    
    public ListNode mergeKLists(ListNode[] lists) {
     
        if(lists.length==0)
            return null;
        
        if(lists.length==1)
            return lists[0];
        
        // comparator as Lambda expression
        PriorityQueue<ListNode> que = new PriorityQueue<ListNode>(lists.length, ( a ,b ) -> a.val - b.val );
    
        
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