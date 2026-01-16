/*
        Given a binary tree, return the vertical order traversal of its nodes values.
        For each node at position (X, Y), its left and right children respectively will be at positions (X-1, Y-1) and (X+1, Y-1).
        Running a vertical line from X = -infinity to X = +infinity, whenever the vertical line touches some nodes, we report the values of the nodes in order from top to bottom (decreasing Y coordinates).
        If two nodes have the same position, then the value of the node that is reported first is the value that is smaller.
        Return an list of non-empty reports in order of X coordinate.  Every report will have a list of values of nodes.

        Example 1:
        Input: [3,9,20,null,null,15,7]
        Output: [[9],[3,15],[20],[7]]
        Explanation: 
                Without loss of generality, we can assume the root node is at position (0, 0):
                Then, the node with value 9 occurs at position (-1, -1);
                The nodes with values 3 and 15 occur at positions (0, 0) and (0, -2);
                The node with value 20 occurs at position (1, -1);
                The node with value 7 occurs at position (2, -2).

        Example 2:
        Input: [1,2,3,4,5,6,7]
        Output: [[4],[2],[1,5,6],[3],[7]]
        Explanation: 
                The node with value 5 and the node with value 6 have the same position according to the given scheme.
                However, in the report "[1,5,6]", the node value of 5 comes first since 5 is smaller than 6.
        
        Note:
        The tree will have between 1 and 1000 nodes.
        Each node's value will be between 0 and 1000.
*/
/**
 * Definition for a binary tree node.
 * public class TreeNode {
 *     int val;
 *     TreeNode left;
 *     TreeNode right;
 *     TreeNode(int x) { val = x; }
 * }
 */
class Solution {
    
        class MinMax{
                private int min;
                private int max;
                
                public MinMax(){
                        this.min = Integer.MAX_VALUE;
                        this.max = Integer.MIN_VALUE;
                }
                public int getMin(){
                        return min;
                }
                public void setMin(int x){
                        min = x;
                }
                
                public int getMax(){
                        return max;
                }
                public void setMax(int x){
                        max = x;
                }
        }
        
        class Point{
                public int y;
                public int val;
                public Point(int y, int val){
                        this.y      = y;
                        this.val    = val;
                }
        }
        
        public List<List<Integer>> verticalTraversal(TreeNode root) {
                Map<Integer, PriorityQueue<Point>> map = new HashMap();
                MinMax minMax = new MinMax();
                traverse(root, 0, 0, map, minMax);
        
                List<List<Integer>> list = new LinkedList();
                for(int i = minMax.getMin(); i <= minMax.getMax(); i++){
                        PriorityQueue<Point> q = map.get(i);
                        List<Integer> l = new LinkedList();
                        while(!q.isEmpty()){
                                l.add(q.poll().val);
                        }
                        list.add(l);
                }
                return list;
        }
        
        private void traverse(TreeNode root, int x, int y, Map<Integer, PriorityQueue<Point>> map, MinMax minMax){
                if(root == null){
                        return;
                }
                if(!map.containsKey(x)){
                        if(minMax.getMin() > x){
                                minMax.setMin(x);
                        } 
                        if(minMax.getMax() < x){
                                minMax.setMax(x);
                        }
                        PriorityQueue<Point> q = new PriorityQueue<Point>((Point p1,Point p2)->{
                                if(p1.y == p2.y){
                                        return p1.val - p2.val;
                                }
                                return p2.y - p1.y;
                        });
                        map.put(x, q);
                }
                PriorityQueue<Point> q = map.get(x);
                q.add(new Point(y, root.val));
                traverse(root.left, x - 1, y - 1, map, minMax);
                traverse(root.right, x + 1, y - 1, map, minMax);
        }
}