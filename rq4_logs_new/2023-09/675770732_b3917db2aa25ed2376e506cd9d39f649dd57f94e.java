package dataStructures.tree;

public class BinarySearchTree extends BinaryTree{

    public BinarySearchTree(int value) {
        super(value);
    }

    public BinarySearchTree() {
        super();
    }

    public void add(int value) {
        addRecursion(root, value);
    }

    private Node addRecursion(Node root, int value) {
        if (root == null) {
            root = new Node(value);
            return root;
        }

        if (value < root.value) {
            root.leftNode = addRecursion(root.leftNode, value);
        } else if (value > root.value) {
            root.rightNode = addRecursion(root.rightNode, value);
        }

        return root;
    }

    public boolean contains(int value){
        return containsRecursive(root, value);
    }
    private boolean containsRecursive(Node curr, int value){
        if(curr == null){
            return false;
        }
        if(value == curr.value){
            return true;
        }
        if(value < curr.value){
            return containsRecursive(curr.leftNode, value);
        } else {
            return containsRecursive(curr.rightNode, value);
        }
    }

}