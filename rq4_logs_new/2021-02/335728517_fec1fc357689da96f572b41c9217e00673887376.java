// https://leetcode.com/problems/merge-k-sorted-lists/
// Time complexity : O(Number of nodes * logN)
// Space complexity : O(Number of nodes)
class Solution {
    public ListNode mergeKLists(ListNode[] lists) {
        PriorityQueue<ListNode> q = new PriorityQueue<>((a,b)->{
            return a.val - b.val;
        });
        
        for(ListNode n : lists){
            if(n != null)
                q.add(n);
        }
            
        
        ListNode dummy = new ListNode(0);
        ListNode res = dummy;
        while(!q.isEmpty()){
            ListNode curr = q.poll();
            dummy.next = curr;
            curr = curr.next;
            
            if(curr != null){
                q.add(curr);
            }
                
            dummy = dummy.next;
        }
        return res.next;
    }
}