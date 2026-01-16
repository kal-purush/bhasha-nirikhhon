// Definition for singly-linked list.
//class ListNode {
//    int val;
//    ListNode next;
//    ListNode(int x) { val = x; }
//}

class Solution {
    public ListNode mergeTwoLists(ListNode l1, ListNode l2) {
        if (l1 == null) {
            return l2;
        }
        else if (l2 == null) {
            return l1;
        }

        ListNode curr1 = l1;
        ListNode curr2 = l2;
        ListNode merged = null;
        ListNode curr = null;
        while (curr1 != null && curr2 != null) {
//            printList(merged);
            if (curr1.val > curr2.val) {
                if (merged == null) {
                    merged = new ListNode(curr2.val);
                    curr = merged;
                }
                else {
                    ListNode temp = new ListNode(curr2.val);
                    curr.next = temp;
                    curr = temp;
                }
                curr2 = curr2.next;
            }
            else {
                if (merged == null) {
                    merged = new ListNode(curr1.val);
                    curr = merged;
                }
                else {
                    ListNode temp = new ListNode(curr1.val);
                    curr.next = temp;
                    curr = temp;
                }
                curr1 = curr1.next;
            }

        }

        if (curr1 != null) {
            curr.next = curr1;
        }
        else if (curr2 != null) {
            curr.next  =curr2;
        }

        return merged;
    }

    public void printList(ListNode list) {
        ListNode curr = list;
        while (curr != null) {
            System.out.print(curr.val + " -> ");
            curr = curr.next;
        }
        System.out.println();
    }
}

class Test {
    public static void main(String[] args) {
        Solution s = new Solution();

        ListNode n1 = new ListNode(1);
        ListNode n2 = new ListNode(2);
        ListNode n3 = new ListNode(4);
        n1.next = n2;
        n2.next = n3;
        n3.next = null;

        ListNode l1 = new ListNode(1);
        ListNode l2 = new ListNode(3);
        ListNode l3 = new ListNode(4);
        l1.next = l2;
        l2.next = l3;
        l3.next = null;

        s.printList(n1);
        s.printList(l1);

        ListNode n = s.mergeTwoLists(l1, n1);

        s.printList(n);
    }
}