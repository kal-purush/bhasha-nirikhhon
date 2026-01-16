package com.github.lucaseo90.easy;

import com.github.lucaseo90.ListNode;

public class No0021 {
    public ListNode mergeTwoLists(ListNode list1, ListNode list2) {
        ListNode head = new ListNode(-1);
        ListNode curr = head;
        if (list1 == null) {
            return list2;
        } else if (list2 == null) {
            return list1;
        } else {
            while (list1 != null && list2 != null) {
                if (list1.val > list2.val) {
                    curr.next = list2;
                    list2 = list2.next;
                } else {
                    curr.next = list1;
                    list1 = list1.next;
                }
                curr = curr.next;
            }

            if (list1 != null) {
                curr.next = list1;
            } else {
                curr.next = list2;
            }
        }
        return head.next;
    }
}