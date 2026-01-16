package hw.lesson6;

public class Node<T extends Comparable<? super T>> {

    private final T value;

    private Node<T> leftChild;
    private Node<T> rightChild;

    public Node(T value) {
        this.value = value;
    }

    public T getValue() {
        return value;
    }

    public Node<T> getLeftChild() {
        return leftChild;
    }

    public void setLeftChild(Node<T> leftChild) {
        this.leftChild = leftChild;
    }

    public Node<T> getRightChild() {
        return rightChild;
    }

    public void setRightChild(Node<T> rightChild) {
        this.rightChild = rightChild;
    }

    public boolean isLeaf() {
        return leftChild == null && rightChild == null;
    }

    public void addChild(Node<T> child) {
        if (child == null) {
            return;
        }
        if (isRightChild(child.getValue())) {
            setRightChild(child);
        } else {
            setLeftChild(child);
        }
    }

    public boolean isRightChild(T value) {
        return value.compareTo(this.value) > 0;
    }

    public boolean hasOnlyOneChild() {
        return leftChild == null ^ rightChild == null;
    }
}