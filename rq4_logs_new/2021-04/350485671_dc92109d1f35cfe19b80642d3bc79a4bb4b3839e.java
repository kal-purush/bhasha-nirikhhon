package challenges.bst;

public class BinarySearchTree<T extends Comparable<T>> extends BinaryTree<T> {
    public BinarySearchTree() {
        super(null);
    }

    private void insertInto(T item, BTNode<T> node) {
        if (item.compareTo(node.value) < 0) {
            if (node.left == null) {
                node.left = new BTNode<>(item);
            }
        }
    }

    public void add(T item) {
        if (root == null) {
            root = new BTNode<>(item);
            return;
        }

        BTNode<T> current = root;
        while (current != null) {
            if (item.compareTo(current.value) <= 0) {
                if (current.left == null) {
                    current.left = new BTNode<>(item);
                    return;
                } else {
                    current = current.left;
                }
            }

            if (item.compareTo(current.value) > 0) {
                if (current.right == null) {
                    current.right = new BTNode<>(item);
                    return;
                } else {
                    current = current.right;
                }
            }
        }
    }

    public boolean contains(T item) {
        if (root == null) return false;
        BTNode<T> current = root;
        int comp;
        while (true) {
            comp = item.compareTo(current.value);
            if (comp < 0) {
                if (current.left == null) return false;
                current = current.left;
            }
            if (comp == 0) return true;
            if (comp > 0) {
                if (current.right == null) return false;
                current = current.right;
            }
        }
    }
}