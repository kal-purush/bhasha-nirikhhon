/*
 * @lc app=leetcode id=314 lang=java
 *
 * [314] Binary Tree Vertical Order Traversal
 */

// @lc code=start
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
class Pair {
    TreeNode node;
    int col;
    int row;

    Pair(TreeNode node, int row, int col) {
        this.node = node;
        this.col = col;
        this.row = row;
    }
}

class Solution {
    public List<List<Integer>> verticalOrder(TreeNode root) {
        

        List<List<Integer>> res = new ArrayList<>();

        if(root == null) return res;

        Queue<Pair> queue = new LinkedList<>();
        TreeMap<Integer, List<Pair>> treeMap = new TreeMap<>(); 

        queue.add(new Pair(root, 0, 0));

        while(!queue.isEmpty()){

            int levelSize = queue.size();
            int minPosition = queue.peek().col;

            int firstPos = 0, lastPos = 0;

            for(int i = 0; i < levelSize; i++){

                Pair currPair = queue.poll();
                TreeNode currNode = currPair.node;
                int col = currPair.col;
                int row = currPair.row;

                treeMap.putIfAbsent(col, new ArrayList<>());
                treeMap.get(col).add(new Pair(currNode, row, col));
                
                if(currNode.left != null) queue.add(new Pair(currNode.left, row + 1, col - 1));
                if(currNode.right != null) queue.add(new Pair(currNode.right, row + 1, col + 1));

            }

            

        }

        for(int key: treeMap.keySet()){

            List<Pair> nodeList = treeMap.get(key);

            // nodeList.sort( (a, b) -> {
            //     if(a.row != b.row){
            //         return a.row - b.row;
            //     } else{
            //         return a.node.val - b.node.val;
            //     }
            // });

            List<Integer> column = new ArrayList<>();
            for (Pair pair: nodeList){
                column.add(pair.node.val);

            }

            res.add(column);


        }


        return res;
    

    }
}
// @lc code=end
