package daohuei.leetcodelizard;

import static org.junit.Assert.assertTrue;

import org.junit.Test;
import daohuei.leetcodelizard.utils.ListNode;

public class AddTwoNumbersTest {
    @Test
    public void testOnAddTwoNumbersTest() {
        ListNode l1, l2, ans;
        l1 = new ListNode(2, new ListNode(4, new ListNode(3)));
        l2 = new ListNode(5, new ListNode(6, new ListNode(4)));
        ans = new ListNode(7, new ListNode(0, new ListNode(8)));
        assertTrue(ans.equals(AddTwoNumbers.addTwoNumbers(l1, l2)));
        l1 = new ListNode(0);
        l2 = new ListNode(0);
        ans = new ListNode(0);
        assertTrue(ans.equals(AddTwoNumbers.addTwoNumbers(l1, l2)));
        l1 = new ListNode(9,
                new ListNode(9, new ListNode(9, new ListNode(9, new ListNode(9, new ListNode(9, new ListNode(9)))))));
        l2 = new ListNode(9, new ListNode(9, new ListNode(9, new ListNode(9))));
        ans = new ListNode(8, new ListNode(9,
                new ListNode(9, new ListNode(9, new ListNode(0, new ListNode(0, new ListNode(0, new ListNode(1))))))));
        assertTrue(ans.equals(AddTwoNumbers.addTwoNumbers(l1, l2)));
    }
}