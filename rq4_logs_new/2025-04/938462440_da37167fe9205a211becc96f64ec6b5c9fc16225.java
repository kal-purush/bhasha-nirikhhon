package strucky.linkedlist;

public class MergeLinkedList {

    //  Time complexity: O(n)
    //  Space complexity:
    //          Extra: O(1)
    //          Algo: O(1)
    //  Pattern: Dummy head
    public static Node<Integer> mergeLists(Node<Integer> head1, Node<Integer> head2) {
        Node<Integer> dummyHead = new Node<>(0);

        Node<Integer> current1 = head1;
        Node<Integer> current2 = head2;

        Node<Integer> tailOfNewLL = dummyHead;

        while (current1 != null && current2 != null) {

            if (current1.value < current2.value) {
                tailOfNewLL.next = current1;
                current1 = current1.next;
            }  else {
                tailOfNewLL.next = current2;
                current2 = current2.next;
            }

            tailOfNewLL = tailOfNewLL.next;
        }

        if (current1 != null) tailOfNewLL.next = current1;
        if (current2 != null) tailOfNewLL.next = current2;

        return dummyHead.next;
    }

    //  Time complexity: O(n)
    //  Space complexity:
    //          Extra: O(n)
    //          Algo: O(1)
    //  Pattern: Recursion
    public static Node<Integer> mergeListsWithRecursion(Node<Integer> head1, Node<Integer> head2) {
        if (head1 == null) return head2;
        if (head2 == null) return head1;

        if (head1.value < head2.value) {
            head1.next = mergeLists(head1.next, head2);
            return head1;
        } else {
            head2.next = mergeLists(head1, head2.next);
            return head2;
        }
    }
}