package br.dev.ferreiras.challenges.leetCode;
/*
You are given two linked lists: list1 and list2 of sizes n and m respectively.
Remove list1's nodes from the ath node to the bth node, and put list2 in their place.

Example 1:
Input: list1 = [10,1,13,6,9,5], a = 3, b = 4, list2 = [1000000,1000001,1000002]
Output: [10,1,13,1000000,1000001,1000002,5]
Explanation:
We remove the nodes 3 and 4 and put the entire list2 in their place.

Example 2:
Input: list1 = [0,1,2,3,4,5,6], a = 2, b = 5, list2 = [1000000,1000001,1000002,1000003,1000004]
Output: [0,1,1000000,1000001,1000002,1000003,1000004,6]


Constraints:

3 <= list1.length <= 104
1 <= a <= b < list1.length - 1
1 <= list2.length <= 104
 */
public class MergeLinkedList {
  public ListNode mergeInBetween(ListNode list1, int a, int b, ListNode list2) {
    // Initialize current node to traverse list1
    ListNode currentNode = list1;
    // Initialize index to keep track of current position in list1
    int index = 0;

    // Traverse list1 until the node before ath node
    while (index < a - 1) {
      currentNode = currentNode.next;
      index++;
    }
    // Store the node before ath node
    ListNode leftNode = currentNode;

    // Traverse list1 from ath node to (b+1)th node
    while (index < b + 1) {
      currentNode = currentNode.next;
      index++;
    }
    // Store the node after bth node
    ListNode rightNode = currentNode;

    // Find the tail of list2
    ListNode secondListTail = list2, secondListHead = list2;
    while (secondListTail.next != null) {
      secondListTail = secondListTail.next;
    }

    // Connect the leftNode node to the head of list2
    leftNode.next = secondListHead;
    // Connect the tail of list2 to the rightNode node
    secondListTail.next = rightNode;

    // Return the modified list1
    return list1;
  }
}