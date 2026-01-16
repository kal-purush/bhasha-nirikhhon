package algorithm;

import algorithm.model.ListNode;

import java.util.HashSet;

/**
 * @author lanche
 * @Date 2020/7/26
 * @Description
 */
//给定一个链表，判断链表中是否有环。
//
// 为了表示给定链表中的环，我们使用整数 pos 来表示链表尾连接到链表中的位置（索引从 0 开始）。 如果 pos 是 -1，则在该链表中没有环。
//
//
//
// 示例 1：
//
// 输入：head = [3,2,0,-4], pos = 1
//输出：true
//解释：链表中有一个环，其尾部连接到第二个节点。
//
//
//
//
// 示例 2：
//
// 输入：head = [1,2], pos = 0
//输出：true
//解释：链表中有一个环，其尾部连接到第一个节点。
//
//
//
//
// 示例 3：
//
// 输入：head = [1], pos = -1
//输出：false
//解释：链表中没有环。
//
//
//
//
//
//
// 进阶：
//
// 你能用 O(1)（即，常量）内存解决此问题吗？
// Related Topics 链表 双指针
public class HasCycle {

    /**
     * 使用hashset判重检测是否有环
     * @param head
     * @return
     */
    public boolean hasCycleUseHashSet(ListNode head) {
        HashSet<ListNode> set = new HashSet<>();
        ListNode cur = head;
        while (cur != null) {
            if (set.contains(cur)) {
                return true;
            } else {
                set.add(cur);
            }
        }
        return false;
    }

    /**
     * 双指针法，通过一块一慢的两个指针不断迭代判断是否会重合来判断是否有环
     * 因为while循环的条件为两个节点不相等，所以起始的快慢指针设值应该不一样
     * @param head
     * @return
     */
    public boolean hasCycleUseTwoPointer(ListNode head) {
        if (head == null || head.next == null) {
            return false;
        }
        ListNode slow = head;
        ListNode fast = head.next;
        while (slow != fast) {
            if (fast == null || fast.next == null) {
                return false;
            }
            slow = slow.next;
            fast = fast.next.next;
        }
        return true;
    }
}