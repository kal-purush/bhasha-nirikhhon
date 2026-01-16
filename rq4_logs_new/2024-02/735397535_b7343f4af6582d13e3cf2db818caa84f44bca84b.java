package model;

public class Node {

    private Task task;
    private Node prev;
    private Node next;

    public Node(Node prev, Task task, Node next) {
        this.prev = prev;
        this.task = task;
        this.next = next;
    }

    public Task getTask() {
        return task;
    }

    public void setTask(Task task) {
        this.task = task;
    }

    public Node getPrev() {
        return prev;
    }

    public void setPrev (Node prev) {
        this.prev = prev;
    }

    public Node getNext() {
        return next;
    }

    public void setNext(Node next) {
        this.next = next;
    }

    @Override
    public String toString() {
        return "Node={" +
                "task=" + task.getId() + ", prev= \'" + prev.getTask().getName() +
                "\'" + ", next= \'" + next.getTask().getName() + "\'" + '}';
    }

}