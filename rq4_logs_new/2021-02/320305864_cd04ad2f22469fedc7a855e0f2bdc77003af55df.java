package daohuei.leetcodelizard;

import daohuei.leetcodelizard.utils.ListNode;

public class AddTwoNumbers {

    public static ListNode addTwoNumbers(ListNode l1, ListNode l2) {
        ListNode dummyHead = new ListNode();
        ListNode ptr1 = l1;
        ListNode ptr2 = l2;
        ListNode ans = dummyHead;
        int carry = 0;
        while (ptr1 != null || ptr2 != null) {
            int val1 = 0;
            int val2 = 0;
            if (ptr1 != null) {
                val1 = ptr1.val;
                ptr1 = ptr1.next;
            }
            if (ptr2 != null) {
                val2 = ptr2.val;
                ptr2 = ptr2.next;
            }
            int sum = val1 + val2 + carry;
            ans.next = new ListNode(sum % (10));
            ans = ans.next;
            carry = sum / (10);
        }
        if (carry != 0) {
            ans.next = new ListNode(carry);
        }

        return dummyHead.next;
    }
}