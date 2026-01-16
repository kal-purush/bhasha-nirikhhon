package com.java.studyplan.leetcode;

import org.junit.Test;

/**
 * @author xuguoqi
 * @desc 给出两个 非空 的链表用来表示两个非负的整数。其中，它们各自的位数是按照 逆序 的方式存储的，并且它们的每个节点只能存储 一位 数字。
 * 如果，我们将这两个数相加起来，则会返回一个新的链表来表示它们的和
 * @date 2020/5/19 15:01
 */
public class AddTwoNumbers {

    /**
     * 输入：(2 -> 4 -> 3) + (5 -> 6 -> 4)
     * 输出：7 -> 0 -> 8
     * 原因：342 + 465 = 807
     *
     * @param l1
     * @param l2
     * @return
     */
    public ListNode addTwoNumbers(ListNode l1, ListNode l2) {
        ListNode dummyHead = new ListNode(0);
        ListNode p = l1, q = l2, curr = dummyHead;
        int carry = 0;
        while (p != null || q != null) {
            int x = (p != null) ? p.val : 0;
            int y = (q != null) ? q.val : 0;
            int sum = carry + x + y;
            carry = sum / 10;
            int xxx = sum % 10;
            curr.next = new ListNode(xxx);
            curr = curr.next;
            if (p != null) {
                p = p.next;
            }
            if (q != null){
                q = q.next;
            }
        }
        if (carry > 0) {
            curr.next = new ListNode(carry);
        }
        return dummyHead.next;

    }

    public class ListNode {
        int val;

        ListNode next;

        ListNode(int x) {
            val = x;
        }

        public ListNode getNext() {
            return next;
        }

        public void setNext(ListNode next) {
            this.next = next;
        }
    }

    @Test
    public void test1() {
        ListNode node1 = new ListNode(2);
        ListNode node2 = new ListNode(4);
        ListNode node3 = new ListNode(3);
        node1.setNext(node2);
        node2.setNext(node3);

        ListNode node4 = new ListNode(5);
        ListNode node5 = new ListNode(6);
        ListNode node6 = new ListNode(4);
        node4.setNext(node5);
        node5.setNext(node6);
        ListNode listNode = addTwoNumbers(node1, node4);
        System.out.println(listNode);

    }
}