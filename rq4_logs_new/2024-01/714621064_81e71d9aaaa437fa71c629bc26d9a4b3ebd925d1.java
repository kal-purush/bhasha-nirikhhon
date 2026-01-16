package LeetCode_Easy;

import nodes.ListNode;

import java.util.ArrayList;
import java.util.List;

public class Merge_Two_Sorted_Lists {

    public static void main(String[] args) {
        ListNode l1 = new ListNode(1, new ListNode(2, new ListNode(4)));
        ListNode l2 = new ListNode(1, new ListNode(3, new ListNode(4)));

        System.out.println(mergeTwoLists(l1, l2));
    }

    public static ListNode mergeTwoLists(ListNode list1, ListNode list2) {

        List<Integer> resultList = new ArrayList<>();


        while (list1 != null || list2 != null) {

            if (list1 == null) {
                resultList.add(list2.val);
                list2 = list2.next;
                continue;
            } else if (list2 == null) {
                resultList.add(list1.val);
                list1 = list1.next;
                continue;
            }

            if (list1.val > list2.val) {
                resultList.add(list2.val);
                list2 = list2.next;
            } else if (list2.val > list1.val) {
                resultList.add(list1.val);
                list1 = list1.next;
            } else {
                resultList.add(list1.val);
                list1 = list1.next;

                resultList.add(list2.val);
                list2 = list2.next;
            }

        }

        ListNode result = new ListNode();

        for (int num : resultList) {
            ListNode newNode = new ListNode(num);
            ListNode current = result;

            while (current.next != null) {
                current = current.next;
            }

            current.next = newNode;
        }

        return result.next;
    }

}