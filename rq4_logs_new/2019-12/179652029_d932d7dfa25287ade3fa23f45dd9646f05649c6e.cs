using System;
using Microsoft.VisualStudio.TestTools.UnitTesting;

namespace UnitMSTest
{

    /*Merge two sorted linked lists and return it as a new list.The new list should be made by splicing together the nodes of the first two lists.

    Example:

    Input: 1->2->4, 1->3->4
    Output: 1->1->2->3->4->4*/

    [TestClass]
    public class MergeTwoSortedLists
    {
        public class ListNode
        {
                 public int val;
                 public ListNode next;
                 public ListNode(int x) { val = x; }
        }

        public ListNode MergeTwoLists(ListNode l1, ListNode l2)
        {
            if (l1 == null)
            {
                return l2;
            }
            if (l2 == null)
            {
                return l1;
            }
            if (l1.val < l2.val)
            {
                l1.next = MergeTwoLists(l1.next, l2);
                return l1;
            }
            else
            {
                l2.next = MergeTwoLists(l1, l2.next);
                return l2;
            }
        }

        [TestMethod]
        public void TestMethod1()
        {
        }
    }
}