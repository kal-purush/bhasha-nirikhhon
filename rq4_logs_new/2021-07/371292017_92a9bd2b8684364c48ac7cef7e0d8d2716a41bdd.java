package Array;

/**
 * @Author ken_xing
 * @Date 2021/7/12--11:22
 * @Version 1.0
 * @Description 合并两个排序后的单链表，合并后依然是正确的排序规则
 */
public class MergeTwoSortedLists {

    public ListNode MergeTwoLists(ListNode l1,ListNode l2)
    {
        if (l1==null)
        {
            return l2;
        }
        if (l2==null)
        {
            return l1;
        }

        if (l1.val< l2.val)
        {
            //递归后续判断
            l1.next=MergeTwoLists(l1.next,l2);
            return l1;
        }
        else
        {

            //递归后续判断
            l2.next=MergeTwoLists(l1,l2.next);
            return l2;
        }
    }
}

//自动一链表
public class ListNode{
    public int val;
    public ListNode next;
    public ListNode(int val=0,ListNode next=null)
    {
        this.val=val;
        this.next=next;
    }
}