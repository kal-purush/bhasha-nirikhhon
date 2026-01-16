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
class MergeKSortedLists {
    public ListNode mergeKLists(ListNode[] lists) {
        if(lists.length == 0)
            return null;
        return demerge(lists,0,lists.length-1);
    }

    public ListNode demerge(ListNode[] lists, int lo, int hi){
        if(lo == hi){
            return lists[lo];
        }
        else if(lo < hi){
            int m = (lo+hi)/2;
            ListNode l1 = demerge(lists,m+1,hi);
            ListNode l2 = demerge(lists,lo,m);
            return merge(l1,l2);
        }
        else {
            return null;
        }
    }

    public ListNode merge(ListNode l1, ListNode l2){
        ListNode l3 = new ListNode(0);
        ListNode head = l3;
        while(l1 != null && l2 != null) {
            if(l1.val < l2.val){
                l3.next = new ListNode(l1.val);
                l1 = l1.next;
            }
            else {
                l3.next = new ListNode(l2.val);
                l2 = l2.next;
            }
            l3 = l3.next;
        }
        while(l1 != null){
            l3.next = new ListNode(l1.val);
            l1 = l1.next;
            l3 = l3.next;
        }
        while(l2 != null){
            l3.next = new ListNode(l2.val);
            l2 = l2.next;
            l3 = l3.next;
        }
        return head.next;
    }
}

// same has merge sort. same technique.
// time complexity O(nlogn)
//space complexity O(n)