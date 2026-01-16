package com.github.lucaseo90.algo.day05.twopointer;

import com.github.lucaseo90.ListNode;

public class No0019 {
    public ListNode removeNthFromEnd(ListNode head, int n) {
        if (head == null) {
            return null;
        }

        ListNode dummy = new ListNode(-1);
        dummy.next = head;

        ListNode slow = dummy;
        ListNode fast = dummy;
        int counter = 0;

        while (counter <= n) {
            fast = fast.next;
            counter++;
        }

        while (fast != null) {
            slow = slow.next;
            fast = fast.next;
        }

        slow.next = slow.next.next;

        return dummy.next;
    }
}