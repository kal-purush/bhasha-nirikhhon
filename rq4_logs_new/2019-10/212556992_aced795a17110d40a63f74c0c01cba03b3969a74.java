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
        int num1 = convert2Int(l1);
        int num2 = convert2Int(l2);

        int sum = num1 + num2;
        return convert2List(sum);
    }

    public int convert2Int(ListNode head) {
        if (head.next == null) {
            return head.val;
        }

        int result = 0;
        ListNode curr = head;
        while (curr.next != null) {
            result = result * 10 + curr.val;
        }
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
            curr.val = temp % 10;
            curr.next = null;
           
            if (head == null) {
                head = curr;
                previous = new ListNode(0);
                previous.next = head;
                temp /= 10;
                continue;
            }

            // previous
            previous = previous.next;
            previous.next = curr;
            

            temp = temp / 10;
        }

        return head;
    }
}


class Test1 {
    public static void main(String[] args) {
        Solution s = new Solution();
   
    
        // ListNode head = s.convert2List(0);
        // ListNode curr = head;
        // while (curr.next != null) {
        //     System.out.println(curr.val);
        //     curr = curr.next;
        // }
        // System.out.println(curr.val);

        // ListNode l1 = s.convert2List(342);
        // ListNode l2 = s.convert2List(465);

        // ListNode head = s.addTwoNumbers(l1, l2);
        // ListNode curr = head;
        // while (curr.next != null) {
        //     System.out.println(curr.val);
        //     curr = curr.next;
        // }
        // System.out.println(curr.val);

        ListNode l1 = new ListNode(1);
        ListNode curr = new ListNode(2);
        curr.next = null;
        l1.next = curr;

        int n = s.convert2Int(l1);
        System.out.println(n);
    }



}