package mytest.algorithm.doublepoint;

/**
 * // 给你一个链表和一个特定值 x ，请你对链表进行分隔，使得所有小于 x 的节点都出现在大于或等于 x 的节点之前。
 * //
 * // 你应当保留两个分区中每个节点的初始相对位置。
 * // 示例：
 * //
 * //输入：head = 1->4->3->2->5->2, x = 3
 * //输出：1->2->2->4->3->5 //小于3的都放左边，不小于3的都放右边
 * // https://leetcode-cn.com/problems/partition-list/
 *
 * @author : yufei
 * @date : 2021/1/20 15:06
 * @description :
 */
public class PartitionList {
    // 设置两个链表，一个存放小于的，一个存放大于的
    public ListNode partition(ListNode head, int x) {
        if (head == null || head.next == null) {
            return head;
        }
        ListNode left = new ListNode(0); //使用原始节点会导致循环引用
        ListNode leftHead = left;
        ListNode right = new ListNode(0);
        ListNode rightHead = right;
        while (head != null) {
            if (head.val < x) {
                left.next = head;
                left = left.next;
            } else {
                right.next = head;
                right = right.next;
            }
            head = head.next;
        }
        right.next = null; //大值链表最后一个节点可能仍有引用，需要删掉
        left.next = rightHead.next;

        return leftHead.next;
    }

    public class ListNode {
        int val;
        ListNode next;

        ListNode() {
        }

        ListNode(int val) {
            this.val = val;
        }

        ListNode(int val, ListNode next) {
            this.val = val;
            this.next = next;
        }
    }
}