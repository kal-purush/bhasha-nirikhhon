package lib.datastructure;

import java.lang.reflect.Array;
import java.util.ArrayDeque;

class CNode {
    int val;
    CNode left;
    CNode right;

    public CNode(int val) {
        this.val = val;
		this.left = null;
		this.right = null;
    }
}

public class ReorderForBST {
	// [1,2,3] becomes [2,1,3]
    public void sortedArrayToBSTArray(int[] nums) {
        if (nums == null || nums.length == 0) {
			return;
        }
        CNode root = constructBST(nums, 0, nums.length - 1);
		int[] count = new int[]{0};
		ArrayDeque<CNode> queue = new ArrayDeque<>();

		constructBSTArray(root, queue, count, nums);
    }

    private CNode constructBST(int[] nums, int left, int right) {
        if (left > right) {
            return null;
        }
        int mid = left + (right - left) / 2;
        CNode root = new CNode(nums[mid]);
        root.left = constructBST(nums, left, mid - 1);
        root.right = constructBST(nums, mid + 1, right);
        return root;
    }

	private void constructBSTArray(CNode node, ArrayDeque<CNode> q, int[] count, int[] nums){
		if(node == null){
			return;
		}

		nums[count[0]] = node.val;
		count[0] += 1;

		if(node.left != null){
			q.addLast(node.left);
		}

		if(node.right != null){
			q.addLast(node.right);
		}

		if(q.isEmpty() == false){
			constructBSTArray(q.removeFirst(), q, count, nums);
		}else{
			return;
		}
	}

    public void inorderTraversal(CNode root) {
        if (root == null) {
            return;
        }
        inorderTraversal(root.left);
        System.out.print(root.val + " ");
        inorderTraversal(root.right);
    }
}
