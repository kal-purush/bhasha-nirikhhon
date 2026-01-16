/*
        You are given a binary tree in which each node contains an integer value.
        Find the number of paths that sum to a given value.
        The path does not need to start or end at the root or a leaf, but it must go downwards (traveling only from parent nodes to child nodes).
        The tree has no more than 1,000 nodes and the values are in the range -1,000,000 to 1,000,000.

        Example:
        root = [10,5,-3,3,2,null,11,3,-2,null,1], sum = 8

                         10
                        /  \
                       5   -3
                      / \    \
                     3   2   11
                    / \   \
                   3  -2   1

        Return 3. The paths that sum to 8 are:

        1.  5 -> 3
        2.  5 -> 2 -> 1
        3. -3 -> 11
*/
/**
 * Definition for a binary tree node.
 * public class TreeNode {
 *     TreeNode left;
 *     TreeNode right;
 *     TreeNode(int x) { val = x; }
 * }
 */
class Solution {
        private int count;
        public int pathSum(TreeNode root, int sum) {
                if(root == null)
                        return 0;
                count = 0;
                Map<Integer, Integer> map = new HashMap();
                map.put(0, 1);
                pathSumF(root, 0, sum, map);
                return count;
                // return pathSumFrom(root, sum) + pathSum(root.left, sum) + pathSum(root.right, sum);
        }
        
        private int pathSumFrom(TreeNode root, int sum){
                if(root == null)
                        return 0;
                return ((root.val == sum) ? 1 : 0 ) + pathSumFrom(root.left, sum - root.val) + pathSumFrom(root.right, sum - root.val);
        }
        
        private void pathSumF(TreeNode root, int curSum, int sum, Map<Integer, Integer> map){
                if(root == null){
                        return;
                }
                curSum += root.val;
                if(map.containsKey(curSum - sum)){
                        count += map.get(curSum - sum);
                }
                map.put(curSum, map.getOrDefault(curSum, 0) + 1);
                pathSumF(root.left, curSum, sum, map);
                pathSumF(root.right, curSum, sum, map);
                map.put(curSum, map.get(curSum)-1);
        }
}