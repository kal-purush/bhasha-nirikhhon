/**
*@author Pranay Sampat
*/

/** PostOrder traversal
*   Iterative Solution
*/
---------------------------------------------------------------------------------------------------------
/**
 * Definition for a binary tree node.
 * public class TreeNode {
 *     int val;
 *     TreeNode left;
 *     TreeNode right;
 *     TreeNode(int x) { val = x; }
 * }
 */
public class Solution {
    public List<Integer> postorderTraversal(TreeNode root) {
        List<Integer> result = new ArrayList<>();
        if(root == null)
            return result;
        Stack<TreeNode> stack = new Stack<TreeNode>();
        stack.push(root);
        TreeNode previous = null;
        while(!stack.isEmpty()){
            TreeNode current = stack.peek();
            if(previous == null || previous.left == current || previous.right == current){
                if(current.left != null)
                    stack.push(current.left);
                else if(current.right != null)
                    stack.push(current.right);
            }else if(current.left == previous){
                if(current.right != null)
                    stack.push(current.right);
            }else{
                result.add(current.val);
                stack.pop();
            }
            previous = current;
        }
        return result;
    }

    // Recursive Solution
    public void postOrderTraversal(TreeNode root){
        if(root == null)
            return null;
    }
    while(root != null){
        postorderTraversal(root.left);
        postorderTraversal(root.right);
        System.out.print(root.data + "");
    }
}

// Preorder traversal

// Iterative Solution
public class Solution {
    public List<Integer> preorderTraversal(TreeNode root) {
       if(root == null){
           return new ArrayList<Integer>();
       }
       List<Integer> arrayList = new ArrayList<Integer>();
        while(root != null){
            arrayList.add(root.val);
            arrayList.addAll(preorderTraversal(root.left));
            arrayList.addAll(preorderTraversal(root.right));
        }
        return arrayList;
    }
}

//Recursive Solution
public void preorderTraversal(TreeNode root){
        if(root == null)
            return null;
    }
    while(root != null){
      
        System.out.print(root.data + "");
          preorderTraversal(root.left);
          preorderTraversal(root.right);

    }
}

// Inorder inorderTraversal

//Iterative Solution
public class Solution {
    public List<Integer> inorderTraversal(TreeNode root) {
        List<Integer> result = new ArrayList<>();
        if(root == null)
            return result;
        boolean done = false;
        Stack<TreeNode> stack = new Stack<>();
        TreeNode currentNode = root;
        while(!done){
            if(currentNode != null){
                stack.push(currentNode);
                currentNode = currentNode.left;
            }else{
                if(stack.isEmpty())
                    done = true;
                else{
                    currentNode = stack.pop();
                    result.add(currentNode.val);
                    currentNode = currentNode.right;
                }
            }
        }
        return result;
    }
}

//Recursive Solution

public void inorderTraversal(TreeNode root){
        if(root == null)
            return null;
    }
    while(root != null){
       inorderTraversal(root.left);
        System.out.print(root.data + "");
         
          inorderTraversal(root.right);

    }
}
