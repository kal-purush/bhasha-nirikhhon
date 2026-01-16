package problem109;

import common.ListNode;
import common.TreeNode;

public final class BalancedTree {

    public TreeNode sortedListToBST(final ListNode head) {
        final int len = calcLen(head);
        return iterateToTree(new ListPtr(head), 0, len - 1);
    }

    private int calcLen(ListNode head) {
        int len = 0;
        while (head != null) {
            len++;
            head = head.next;
        }
        return len;
    }

    private TreeNode iterateToTree(final ListPtr curPtr, final int leftIdx, final int rightIdx) {
        if (rightIdx < leftIdx)
            return null;

        final int midIdx = (leftIdx + rightIdx) / 2;
        final TreeNode left = iterateToTree(curPtr, leftIdx, midIdx - 1);
        final TreeNode curRoot = new TreeNode(curPtr.ptr.val);
        curPtr.ptr = curPtr.ptr.next;
        curRoot.left = left;
        curRoot.right = iterateToTree(curPtr, midIdx + 1, rightIdx);
        return curRoot;
    }

    private static final class ListPtr {
        private ListNode ptr;

        ListPtr(final ListNode ptr) {
            this.ptr = ptr;
        }
    }

}