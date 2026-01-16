package day05;

/**
 * @author halley
 * @version 1.0
 * @description: 构造一个链表
 * @date 2021/12/11 9:32 上午
 */
class ListNode {
    int val;
    ListNode next;

    ListNode(int x) {
        val = x;
    }

    public ListNode(int[] nums) {
        if (nums == null || nums.length == 0) {
            throw new IllegalArgumentException("arr can not be empty");
        }
        this.val = nums[0];
        ListNode curr = this;
        for (int i = 1; i < nums.length; i++) {
            curr.next = new ListNode(nums[i]);
            curr = curr.next;
        }
    }

//    @Override
//    public String toString() {
//        StringBuilder s = new StringBuilder();
//        ListNode cur = this;
//        while (cur != null) {
//            s.append(cur.val);
//            s.append(" -> ");
//            cur = cur.next;
//        }
//        s.append("NULL");
//        return s.toString();
//    }
}