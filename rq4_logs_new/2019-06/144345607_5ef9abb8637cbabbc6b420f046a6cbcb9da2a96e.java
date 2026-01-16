package src.RemoveDuplicatesFromSortedList_83;

import helper.ListNode;

public class Solution {

    //迭代方法
    public ListNode deleteDuplicates(ListNode head) {
        ListNode current = head;
        while (current != null && current.next != null) {
            if (current.val == current.next.val) {
                current.next = current.next.next;
            } else {
                current = current.next;
            }
        }
       return head;
    }

    //递归方式，但是对于链表来说可能并不是个好方法，
    //在随着链表长度无限扩大的时候所需空间是O(n)
    public ListNode deleteDuplicates2(ListNode head) {
        if(head == null || head.next == null)return head;
        head.next = deleteDuplicates2(head.next);
        return head.val == head.next.val ? head.next : head;
    }

}