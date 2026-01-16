package com.hussir.leetcode.part003;

import com.hussir.leetcode.datastructure.ListNode;

/**
 * Description:
 *     Merge two sorted linked lists and return it as a new list. The new list should be made by splicing together the nodes of the first two lists.
 */
public class Question0021 {

    public static ListNode mergeTwoLists(ListNode l1, ListNode l2) {

        if (l1 == null) return l2;
        if (l2 == null) return l1;

        ListNode cursor1 = l1;
        ListNode cursor2 = l2;

        ListNode resultHead;
        if (cursor1.val < cursor2.val) {
            resultHead = cursor1;
            cursor1 = cursor1.next;
        } else {
            resultHead = cursor2;
            cursor2 = cursor2.next;
        }

        ListNode resultCursor = resultHead;

        while (true) {  //cursor1 != null || cursor2 != null

            if (cursor1 == null) {
                resultCursor.next = cursor2;
                break;
            }

            if (cursor2 == null) {
                resultCursor.next = cursor1;
                break;
            }

            if (cursor1.val < cursor2.val) {

                resultCursor.next = cursor1;
                resultCursor = resultCursor.next;

                cursor1 = cursor1.next;
            } else {

                resultCursor.next = cursor2;
                resultCursor = resultCursor.next;

                cursor2 = cursor2.next;
            }

        }

        return resultHead;
    }
}