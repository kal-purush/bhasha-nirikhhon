package exercises.linked.list;

public class SplitLinkedList {
  public ListNode[] splitListToParts(final ListNode head,
                                     final int k) {
    final var result = new ListNode[k];
    final var nodeSize = countSize(head);
    final var sizePerNode = Math.max(nodeSize / k, 1);
    var remaining = nodeSize - Math.min(sizePerNode * k, nodeSize);
    var nextNode = head;

    for (int i = 0; i < k; i++) {
      result[i] = nextNode;
      for (int j = 0; j < sizePerNode - 1 && nextNode != null; j++) {
        nextNode = nextNode.next;
      }

      if (nextNode != null && remaining > 0) {
        nextNode = nextNode.next;
        remaining--;
      }

      if (nextNode != null && nextNode.next != null) {
        final var tmp = nextNode.next;
        nextNode.next = null;
        nextNode = tmp;
      } else {
        nextNode = null;
      }

    }

    return result;
  }

  private int countSize(final ListNode head) {
    if (head == null) {
      return 0;
    } else {
      return 1 + countSize(head.next);
    }
  }
}