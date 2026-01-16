//APPROACH 1: inOrder iterative solution
//time complexity O(n) since we are visiting each node
//space complexity O(n) stack space used is n in worst case

/**
 * Definition for a binary tree node.
 * public class TreeNode {
 *     int val;
 *     TreeNode left;
 *     TreeNode right;
 *     TreeNode() {}
 *     TreeNode(int val) { this.val = val; }
 *     TreeNode(int val, TreeNode left, TreeNode right) {
 *         this.val = val;
 *         this.left = left;
 *         this.right = right;
 *     }
 * }
 */
class Solution {
    TreeNode prev; TreeNode curr; TreeNode first; TreeNode last; boolean flag = false;
    public void recoverTree(TreeNode root) {
        if(root == null) return;
        int temp;
        inOrder(root);
        if(first != null && last != null){
            temp = last.val;
            last.val = first.val;
            first.val = temp;
        }

    }
    private void inOrder(TreeNode root){
        Stack<TreeNode> st = new Stack<>();
        while(root != null || !st.isEmpty()){
            while(root != null){
                st.push(root);
                root = root.left;
            }
            root = st.pop();
            if(prev != null && prev.val > root.val){
                if(!flag){
                    flag = true;
                    first = prev;
                    last = root;
                } else{
                    last = root;
                }
            }
            prev = root;
            root = root.right;
        }
    }
}

//APPROACH 2: recursive solution
//time complexity O(n)
//space complexity O(n) no auxillary space. only recursive stack space

/**
 * Definition for a binary tree node.
 * public class TreeNode {
 *     int val;
 *     TreeNode left;
 *     TreeNode right;
 *     TreeNode() {}
 *     TreeNode(int val) { this.val = val; }
 *     TreeNode(int val, TreeNode left, TreeNode right) {
 *         this.val = val;
 *         this.left = left;
 *         this.right = right;
 *     }
 * }
 */
class Solution {
    TreeNode prev; TreeNode first; TreeNode last; boolean flag = false;
    public void recoverTree(TreeNode root) {
        if(root == null) return;
        int temp;
        inOrder(root);
        if(first != null && last != null){
            temp = last.val;
            last.val = first.val;
            first.val = temp;
        }

    }
    private void inOrder(TreeNode root){
        if(root == null) return;
        inOrder(root.left);
        if(prev != null && prev.val > root.val){
            if(!flag){
                flag = true;
                first = prev;
                last = root;
            } else{
                last = root;
            }
        }
        prev = root;
        inOrder(root.right);
    }
}