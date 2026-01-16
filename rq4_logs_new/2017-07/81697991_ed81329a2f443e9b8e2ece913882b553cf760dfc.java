package demo.interview;


/**
 * Created by Qi on 2017/7/25.
 */

class ListNode{
    int val;
    ListNode next;
    ListNode(int x){val=x;}
}
public class ReverseLinkedList {
    public ListNode reverse(ListNode head){
        if (head==null||head.next==null)
            return head;
        ListNode tail=null;
        ListNode p=head;
        ListNode q=p.next;

        while (p!=null){

            ListNode old=q.next;
            p.next=tail;
            q.next=p;
            tail=p;
            p=q;
            q=old;
        }
      return  p;
    }


}