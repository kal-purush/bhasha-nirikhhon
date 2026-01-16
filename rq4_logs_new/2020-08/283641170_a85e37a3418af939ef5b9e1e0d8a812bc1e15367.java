class MinStack {

    /** initialize your data structure here. */
    private class Node {
        int val;
        int min;
        Node next;
        Node(int val, int min) {
            this.val = val;
            this.min = min;
        }
        Node() {}
    }
    private Node head;
    private Node tail;
    private Node minNode;
    public MinStack() {
        head = new Node();
        tail = new Node();
        head.next = tail;
    }

    void addNode(Node node){
        Node ptr = head.next;
        head.next = node;
        node.next = ptr;
    }

    Node removeNode(){
        if(head.next == tail) {
            return null;
        }
        Node ptr = head.next;
        head.next = ptr.next;
        return ptr;
    }

    public void push(int x) {

        if(head.next == tail){
            Node node = new Node(x,x);
            addNode(node);
        }
        else {
            int min_val = getMin();
            min_val = Math.min(min_val,x);
            Node node = new Node(x,min_val);
            addNode(node);
        }

    }

    public void pop() {
        removeNode();
    }

    public int top() {
        if(head.next == tail) {
            return -1;
        }
        return head.next.val;
    }

    public int getMin() {

        return head.next.min;
    }
}

/**
 * Your MinStack object will be instantiated and called as such:
 * MinStack obj = new MinStack();
 * obj.push(x);
 * obj.pop();
 * int param_3 = obj.top();
 * int param_4 = obj.getMin();
 */

// all operations O(1). space complexity: O(n);
// intuition: Used linked list so for queue, we can add and remove at the head.
// I modified linkedlist to store min value along with its value. so whenever I add a new push, it compares with top node
// assigns itself min value;