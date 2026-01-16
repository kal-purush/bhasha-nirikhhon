namespace LeetCode
{
    public class LinkedListCycle
    {
        public class ListNode
        {
            public int val;
            public ListNode? next =null;
            public ListNode(int x)
            {
                val = x;
            }
        }
        public ListNode DetectCycle(ListNode head)
        {
            HashSet<ListNode> set = new();
            ListNode current = head;
            while (current != null)
            {
                if (set.Contains(current))
                    return current;
                set.Add(current);
                current = current.next;
            }
            return null;
        }
    }
}