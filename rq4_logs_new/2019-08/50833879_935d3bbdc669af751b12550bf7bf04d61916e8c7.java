public class Solution {
    public int minDepth(TreeNode root) {
        if(root==null) return 0;

        ArrayList<TreeNode> buf=new ArrayList<TreeNode>();
        buf.add(root);
        int d=0;

        while(buf.size()>0){
            d++;
            ArrayList<TreeNode> newBuf=new ArrayList<TreeNode>();
            boolean reachLeaf=false;
            for(TreeNode tn: buf){
                if(tn.left!=null)   newBuf.add(tn.left);
                if(tn.right!=null)  newBuf.add(tn.right);
                if(tn.left==null && tn.right==null)   {reachLeaf=true; break;}
            }
            if(reachLeaf)   break;
            buf=newBuf;
        }

        return d;
    }
}

// recursive
public class Solution {
    public int minDepth(TreeNode root) {
        if (root==null) return 0;
        if (root.left==null) return minDepth(root.right) + 1;
        if (root.right==null) return minDepth(root.left) + 1;
        return Math.min(minDepth(root.left), minDepth(root.right)) + 1;
    }
}