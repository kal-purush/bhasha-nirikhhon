package challenges.tree;

public class BTNode<T> {
    public T value;
    public BTNode<T> left;
    public BTNode<T> right;

    public BTNode(T value, BTNode<T> left, BTNode<T> right) {
        this.value = value;
        this.left = left;
        this.right = right;
    }

    public BTNode(T value) {
        this.value = value;
        this.left = null;
        this.right = null;
    }

    public BTNode(T value, BTNode<T> left) {
        this.value = value;
        this.left = left;
        this.right = null;
    }
}