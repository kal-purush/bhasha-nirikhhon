package my.com.app.bfs;

/*
// Definition for a Node.
class Node {
    public int val;
    public Node left;
    public Node right;
    public Node next;

    public Node() {}
    
    public Node(int _val) {
        val = _val;
    }

    public Node(int _val, Node _left, Node _right, Node _next) {
        val = _val;
        left = _left;
        right = _right;
        next = _next;
    }
};
*/

class Solution {
    public Node connect(Node root) {
     if ( root == null)
         return null;
        
        ArrayDeque<Node> que = new ArrayDeque();
        que.add(root);
        
        while( !que.isEmpty()){
            
            int size = que.size();
            
            for ( int i=0 ;i <size; i++) {
                
                Node n = que.poll();
                if (i<size-1)
                    n.next = que.peek();
                else
                    n.next = null;
                
                if ( n.left!=null)
                    que.add(n.left);
                
                if ( n.right!=null)
                    que.add(n.right);
                
            }
        }
        return root;
    }
}