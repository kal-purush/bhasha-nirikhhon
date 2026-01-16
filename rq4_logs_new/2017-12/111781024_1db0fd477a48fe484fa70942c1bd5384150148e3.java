package com.week3.presession;

public class BalancedTree {

	 class TreeNode {
	      int val;
	      TreeNode left;
	      TreeNode right;
	      TreeNode(int x) { val = x; }
	  }
	 
	public static void main(String[] args) {

	}

	public int isBalanced(TreeNode a) {
	    if(isBalancedB(a))
	      return 1;
	    return 0;
	}
	
	public boolean isBalancedB(TreeNode root) {
        if(root==null){
            return true;
        }
        return height(root)!=-1;
    }

	public int height(TreeNode node){
	    if(node==null){
	        return 0;
	    }
	    int lH=height(node.left);
	    if(lH==-1){
	        return -1;
	    }
	    int rH=height(node.right);
	    if(rH==-1){
	        return -1;
	    }
	    if(lH-rH<-1 || lH-rH>1){
	        return -1;
	    }
	    return Math.max(lH,rH)+1;
	}

}