package com.github.lucaseo90.ps.day15;

import com.github.lucaseo90.ListNode;

public class AddTwoNumbers {
    public ListNode addTwoNumbers(ListNode l1, ListNode l2) {
        ListNode answer = new ListNode();
        ListNode sum = answer;

        int remain = 0;
        while (l1 != null || l2 != null) {
            int localSum = 0;
            if (l1 != null) {
                localSum += l1.val;
            }
            if (l2 != null) {
                localSum += l2.val;
            }

            localSum += remain;
            remain = localSum >= 10 ? 1 : 0;
            localSum %= 10;

            sum.next = new ListNode(localSum);
            sum = sum.next;

            if (l1 != null && l1.next != null) {
                l1 = l1.next;
            } else {
                l1 = null;
            }
            if (l2 != null && l2.next != null) {
                l2 = l2.next;
            } else {
                l2 = null;
            }
        }
        if (remain != 0) {
            sum.next = new ListNode(remain);
        }

        return answer.next;
    }
}