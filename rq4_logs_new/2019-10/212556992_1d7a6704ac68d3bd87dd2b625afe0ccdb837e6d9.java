/**
 * Definition for singly-linked list.
 * public class ListNode {
 *     int val;
 *     ListNode next;
 *     ListNode(int x) { val = x; }
 * }
 */


class ListNode {
    int val;
    ListNode next;
    ListNode(int x) { val = x; }
}

class Solution {
    public ListNode addTwoNumbers(ListNode l1, ListNode l2) {
        ListNode curr1 = l1;
        ListNode curr2 = l2;

        ListNode result = null;
        ListNode pre = new ListNode(0);
        boolean carry = false;
        while (curr1 != null || curr2 != null) {
            if (curr1 != null && curr2 != null) {
                int currVal = 0;
                if (carry) {
                    currVal = (curr1.val + curr2.val + 1);
                    carry = currVal > 9 ? true : false;
                    currVal = currVal % 10;

//                    currVal = (curr1.val + curr2.val + 1) > 10 ? (curr1.val + curr2.val + 1 - 10) : (curr1.val + curr2.val + 1);
                }
                else {
                    currVal = curr1.val + curr2.val;
                    carry = currVal > 9 ? true : false;
                    currVal %= 10;
                }

//                    currVal = (curr1.val + curr2.val) > 10 ? (curr1.val + curr2.val - 10) : (curr1.val + curr2.val);

                ListNode curr = new ListNode(currVal);
                curr.next = null;

                if (result == null) {
                    result = curr;
                    pre.next = result;
                }

                pre.next = curr;
                pre = curr;

                curr1 = curr1.next;
                curr2 = curr2.next;
            }
            else if (curr1 == null && curr2 != null) {
                int currVal = 0;
                if (carry) {
                    currVal = (curr2.val + 1);
                    carry = (currVal > 9) ? true : false;
                    currVal %= 10;

//                    int currVal = (curr2.val + 1) > 10 ? (curr2.val + 1 - 10) : (curr2.val + 1);
                    ListNode curr = new ListNode(currVal);

                    pre.next = curr;
                    pre = curr;


                }
                else {
                    pre.next = curr2;
                    break;
                }
                curr2 = curr2.next;
            }
            else if (curr1 != null && curr2 == null) {
                int currVal = 0;
                if (carry) {
                    currVal = (curr1.val + 1);
                    carry = currVal > 9 ? true : false;
                    currVal = currVal % 10;

//                    int currVal = (curr1.val + 1) > 10 ? (curr1.val + 1 - 10) : (curr1.val + 1);
                    ListNode curr = new ListNode(currVal);

                    pre.next = curr;
                    pre = curr;


                }
                else {
                    pre.next = curr1;
                    break;
                }
                curr1 = curr1.next;
            }
            else {
                if (carry) {
                    ListNode curr = new ListNode(1);
                    pre.next = curr;
                    break;
                }
            }


        }

        if (carry) {
            ListNode curr = new ListNode(1);
            pre.next = curr;
        }

        return result;
    }





    public int convert2Int(ListNode head) {
        System.err.println("convert2Int");
//        if (head.next == null) {
//            return head.val;
//        }

        int result = 0;
        ListNode curr = head;
        int count = 0;
        while (curr.next != null) {
//            result = result + curr.val * (int) Math.pow(10, count);
            result = result + curr.val * (int) Math.pow(10, count);
            ++count;
            curr = curr.next;
        }
//        result = result + curr.val * (int) Math.pow(10, count);
        result = result + curr.val * (int) Math.pow(10, count);

        System.err.println("result = " + result);
        return result;
    }

    public ListNode convert2List(int num) {
        if (num == 0) {
            return new ListNode(0);
        }

        int temp = num;

        ListNode head = null;
        ListNode previous = null;
        while (temp > 0) {
            ListNode curr = new ListNode(0);
            curr.val =  temp % 10;
            curr.next = null;

            if (head == null) {
                head = curr;
                previous = new ListNode(0);
                previous.next = head;
                temp = temp / 10;
                continue;
            }

            // previous
            previous = previous.next;
            previous.next = curr;


            temp = temp / 10;
        }

        return head;
    }


    public void printList(ListNode l) {
        ListNode curr = l;
        while (curr.next != null) {
            System.out.print(curr.val + " -> ");
            curr = curr.next;
        }
        System.out.println(curr.val);
    }
}


class Test {
    public static void main(String[] args) {
        Solution s = new Solution();


        // ListNode head = s.convert2List(0);
        // ListNode curr = head;
        // while (curr.next != null) {
        //     System.out.println(curr.val);
        //     curr = curr.next;
        // }
        // System.out.println(curr.val);

         ListNode l1 = s.convert2List(99991);
//         s.printList(l1);
         ListNode l2 = s.convert2List(9);

         ListNode head = s.addTwoNumbers(l1, l2);
         s.printList(head);

//        ListNode l1 = new ListNode(1);
//        ListNode curr = new ListNode(2);
//        curr.next = null;
//        l1.next = curr;
//
//        s.printList(l1);
//        int n = s.convert2Int(l1);
//        System.out.println("n = " + n);
    }



}