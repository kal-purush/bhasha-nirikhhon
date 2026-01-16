package strucky.linkedlist;

public class InsertNode {

    //  Time complexity: O(n)
    //  Space complexity:
    //          Extra: O(1)
    //          Algo: O(1)
    public static <T> Node<T> insertNode(Node<T> head, T targetVal, int index) {

        //  Insertion at first index
        if (index == 0) {
            Node<T> newNode = new Node<>(targetVal);
            newNode.next = head;
            return newNode;
        }

        Node<T> current = head;
        Node<T> prev = null;
        int counter = 0;

        //  true because, we might want to add a node at tail as well, where current actually reaches null.
        while (true) {
            if (counter == index) {
                Node<T> newNode = new Node<>(targetVal);
                newNode.next = current;
                prev.next = newNode;
                return head;
            }
            counter++;
            prev = current;
            current = current.next;
        }
    }
}