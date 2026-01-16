package Leetcode;

/*
        https://leetcode.com/problems/range-sum-of-bst/submissions/
        Date : 18-Nov-2022
 */
public class RangeSum {
    private TreeNode root;

    public static void main(String[] args) {
        RangeSum rs = new RangeSum();
        rs.createBinaryTree();
        System.out.println(rs.rangeSumBST(rs.root,7,15));
    }
    int sum = 0;
    public int rangeSumBST(TreeNode root, int low, int high) {
        if(root == null){
            return 0;
        }
        if(root.data >= low && root.data <= high){
            sum+=root.data;
        }
        rangeSumBST(root.left,low,high);
        rangeSumBST(root.right,low,high);
        return sum;
    }
    public void createBinaryTree(){
        TreeNode ten = new TreeNode(10);
        TreeNode five = new TreeNode(5);
        TreeNode fifteen = new TreeNode(15);
        TreeNode three = new TreeNode(3);
        TreeNode seven = new TreeNode(7);
        TreeNode eighteen = new TreeNode(18);

        root = ten;
        ten.right = fifteen;
        ten.left = five;
        five.left = three;
        five.right = seven;
        fifteen.right=eighteen;

    }

    public class TreeNode{
        private TreeNode root;
        private TreeNode left;
        private TreeNode right;
        private int data;

        public TreeNode(int data){
            this.data = data;
        }
    }


}