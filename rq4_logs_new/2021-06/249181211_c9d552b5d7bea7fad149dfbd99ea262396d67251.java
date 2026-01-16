package com.lee.datastructureandalgorithms.leetcode;

import java.util.Comparator;
import java.util.PriorityQueue;

/**
 * @author i324779
 */
public class Solution23 {

    public ListNode mergeKLists(ListNode[] lists) {
        if (lists == null) {
            return null;
        }

        // 小根堆
        PriorityQueue<ListNode> heap = new PriorityQueue<>(new ListNodeComparator());
        for (ListNode list : lists) {
            if (list != null) {
                heap.add(list);
            }
        }

        if (heap.isEmpty()) {
            return null;
        }

        ListNode head = heap.poll();
        ListNode pre = head;
        if (pre.getNext() != null) {
            heap.add(pre.getNext());
        }

        while (!heap.isEmpty()) {
            ListNode cur = heap.poll();
            pre.setNext(cur);
            pre = cur;
            if (cur.getNext() != null) {
                heap.add(cur.getNext());
            }
        }

        return head;
    }

    static class ListNodeComparator implements Comparator<ListNode> {
        @Override
        public int compare(ListNode o1, ListNode o2) {
            //如果返回负数，则小的放在前面；如果返回整数，则大的放在前面；
            return o1.getData() - o2.getData();
        }
    }
}

