package LAB_C;

class Node {
    int data;
    Node next;

    public Node(int data) {
        this.data = data;
        this.next = null;
    }
}

class CircularLinkedList {
    private Node head;

    public CircularLinkedList() {
        this.head = null;
    }

    public void insertAtLast(int data) {
        Node newNode = new Node(data);
        if (head == null) {
            head = newNode;
            head.next = head;
        } else {
            Node temp = head;
            while (temp.next != head) {
                temp = temp.next;
            }
            temp.next = newNode;
            newNode.next = head;
        }
    }

    public int countNodes() {
        if (head == null) {
            return 0;
        }
        int count = 1;
        Node temp = head;
        while (temp.next != head) {
            count++;
            temp = temp.next;
        }
        return count;
    }

    public static void main(String[] args) {
        CircularLinkedList circularList = new CircularLinkedList();
        circularList.insertAtLast(1);
        circularList.insertAtLast(2);
        circularList.insertAtLast(3);

        System.out.println("Number of nodes in the circular linked list: " + circularList.countNodes());
    }
}
