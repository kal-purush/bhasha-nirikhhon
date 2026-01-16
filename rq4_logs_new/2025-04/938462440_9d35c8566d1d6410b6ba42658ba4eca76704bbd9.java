package strucky.linkedlist;

import java.util.ArrayList;
import java.util.List;

public class LinkedListFunctions {

    public static void printLinkedList(Node head) {
        Node current = head;

        //  If current node != null, then we know there are elements in Linked list still.
        while (current != null) {
            System.out.print(current.value + " -> ");
            current = current.next;
        }
    }

    public static void printLinkedListWithRecursion(Node head) {
        if (head == null) {
            return;
        }
        System.out.print(head.value + " -> ");      //  Current element
        printLinkedListWithRecursion(head.next);    //  Next element for each recursive call
    }

    //  Time complexity: O(n)
    //  Space complexity:
    //          Extra: O(n)
    //          Algo: O(1)
    public static List<String> getLinkedListValuesFrom(Node<String> head) {
        List<String> list = new ArrayList<>();
        Node<String> current = head;

        while (current != null) {
            list.add(current.value);
            current = current.next;
        }

        return list;
    }

    //  Time complexity: O(n)
    //  Space complexity:
    //          Extra: O(n)
    //          Algo: O(1)
    //  Pattern: Recursion
    public static List<String> getLinkedListValuesWithRecursionFrom(Node<String> head) {
        List<String> list = new ArrayList<>();
        getLinkedListValuesWithRecursionFrom(head, list);
        return list;
    }

    public static void getLinkedListValuesWithRecursionFrom(Node<String> head, List<String> list) {
        if (head == null) return;
        list.add(head.value);
        getLinkedListValuesWithRecursionFrom(head.next, list);
    }

    //  Time complexity: O(n)
    //  Space complexity:
    //          Extra: O(1)
    //          Algo: O(1)
    public static int sumOfLinkedListNodesFrom(Node<Integer> head) {
        Node<Integer> current = head;
        int sum = 0;

        while (current != null) {
            sum = sum + current.value;
            current = current.next;
        }
        return sum;
    }

    //  Time complexity: O(n)
    //  Space complexity:
    //          Extra: O(n)
    //          Algo: O(1)
    //  Pattern: Recursion
    public static int sumOfLinkedListNodesWithRecursion(Node<Integer> head) {
        if (head == null) return 0;
        return head.value + sumOfLinkedListNodesWithRecursion(head.next);
    }

    //  Time complexity: O(n)
    //  Space complexity:
    //          Extra: O(1)
    //          Algo: O(1)
    public static <T> boolean linkedListFind(Node<T> head, T target) {
        Node<T> current = head;

        while (current != null) {
            if (current.value == target) {
                System.out.print(target + " -> exists in Linked List!! ");
                return true;
            }
            current = current.next;
        }
        return false;
    }

    //  Time complexity: O(n)
    //  Space complexity:
    //          Extra: O(n)
    //          Algo: O(1)
    //  Pattern: Recursion
    public static <T> boolean linkedListFindUsingRecursion(Node<T> head, T target) {
        if (head == null) return false;
        if (head.value == target) return true;

        //  Now checking above for each element
        return linkedListFindUsingRecursion(head.next, target);
    }

    //  Time complexity: O(n)
    //  Space complexity:
    //          Extra: O(1)
    //          Algo: O(1)
    public static <T> T getNodeValue(Node<T> head, int index) {
        Node<T> current = head;
        int tempIndex = 0;

        while (current != null) {
            if (tempIndex == index) return current.value;
            tempIndex++;
            current = current.next;
        }
        return null;
    }

    //  Time complexity: O(n)
    //  Space complexity:
    //          Extra: O(n)
    //          Algo: O(1)
    //  Pattern: Recursion
    public static <T> T getNodeValueWithRecursion(Node<T> head, int index) {
        if (head == null) return null;
        if (index == 0) return head.value;
        return getNodeValueWithRecursion(head.next, index - 1);
    }

    //  Time complexity: O(n)
    //  Space complexity:
    //          Extra: O(1)
    //          Algo: O(1)
    public static <T> Node<T> reverseLinkedList(Node<T> head) {
        Node<T> current = head;
        Node<T> prev = null;

        while (current != null) {
            Node<T> nextNode = current.next;
            current.next = prev;
            prev = current;
        }
        return prev;
    }

    //  Time complexity: O(n)
    //  Space complexity:
    //          Extra: O(n)
    //          Algo: O(1)
    //  Pattern: Recursion
    public static <T> Node<T> reverseLinkedListWithRecursion(Node<T> head) {
        return reverseLinkedListWithRecursion(head, null);
    }

    public static <T> Node<T> reverseLinkedListWithRecursion(Node<T> head, Node<T> prev) {
        if (head == null) return prev;
        Node<T> nextNode = head.next;
        head.next = prev;
        return reverseLinkedListWithRecursion(nextNode, head);
    }
}