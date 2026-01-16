package br.dev.ferreiras.challenges.leetCode;

/*
 Given the head of a linked list, remove the nth node from the end of the list
 and return its head.
 Input: head = [1,2,3,4,5], n = 2
 Output: [1,2,3,5]
 Example 2:

 Input: head = [1], n = 1
 Output: []
 Example 3:

 Input: head = [1,2], n = 1
 Output: [1]

 * Approach
 * Calculate the size of the Single Linked List.
 * We need to travel to the prev node of the node to be removed
 * thus we perform reduce size by n
 * If the node to be removed is the first node (size == 0) then we can
 * simply return the next node of head since it will be null if the list has only one node.
 * Traverse till the prev node using a loop again
 * Skip the next node by linking the prev node to the next of next node.
 * If not present, assign null.
 * Finally return the head
 */
public class LinkedListRemoveNode {
  static class ListNode {
    int val;
    ListNode next;

    ListNode() {
    }

    ListNode(int val) {
      this.val = val;
    }

    ListNode(int val, ListNode next) {
      this.val = val;
      this.next = next;
    }
  }

  public ListNode removeNthFromEnd(ListNode head, int n) {

    int length = linkedListSize(head);
    int i = 0, iterate = length - n - 1;
    if (iterate == -1) return head.next;
    ListNode curr = head;
    while (i < iterate) {
      curr = curr.next;
      i++;
    }

    curr.next = curr.next.next;
    return head;
  }

  public int linkedListSize(ListNode head) {
    int count = 0;
    if (head == null) return count;
    ListNode curr = head;
    while (curr != null) {
      count++;
      curr = curr.next;
    }
    return count;
  }
}