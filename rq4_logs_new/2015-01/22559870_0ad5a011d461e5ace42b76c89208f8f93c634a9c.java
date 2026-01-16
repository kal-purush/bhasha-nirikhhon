package elements.of.programming.interviews;

/**
 * Created by sharath on 1/13/15.
 *
 * Problem 9.1
 * Find if a binary tree is balanced with O(h) space where h is height of the tree
 */
public class BalancedBinaryTree_9_1 {
    private static <T> int getHeight(BinaryTree<T> n) {
        if(n == null) {
            return -1;
        }

        int leftHeight = getHeight(n.getLeft());
        if(leftHeight == -2) {
            return -2; // left subtree is not balanced
        }

        int rightHeight = getHeight(n.getRight());
        if(rightHeight == -2) {
            return -2; // right subtree is not balanced
        }

        if(Math.abs(leftHeight - rightHeight) > 1) {
            return -2; // current node n is not balanced
        }
        return Math.max(leftHeight, rightHeight) + 1;
    }

    public static <T> boolean isBalancedBinaryTree(BinaryTree<T> n) {
        return getHeight(n) != -2;
    }
    public static void main(String[] args) {
        // balanced binary tree test
        // 3
        // 2 5
        // 1 4 6
        BinaryTree<Integer> root = new BinaryTree<>();
        root.setLeft(new BinaryTree<Integer>());
        root.getLeft().setLeft(new BinaryTree<Integer>());
        root.setRight(new BinaryTree<Integer>());
        root.getRight().setLeft(new BinaryTree<Integer>());
        root.getRight().setRight(new BinaryTree<Integer>());
        assert(isBalancedBinaryTree(root));
        System.out.println(isBalancedBinaryTree(root));
        root = new BinaryTree<>();
        root.setLeft(new BinaryTree<Integer>());
        root.getLeft().setLeft(new BinaryTree<Integer>());
        assert(!isBalancedBinaryTree(root));
        System.out.println(isBalancedBinaryTree(root));
    }
}