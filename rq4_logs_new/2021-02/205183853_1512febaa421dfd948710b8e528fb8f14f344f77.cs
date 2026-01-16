
using System;
namespace Linear_DataStructures
{
    public class AddNumbers
    {

        /* Definition for singly-linked list.*/
        public class ListNode
        {
            public int val;
            public ListNode next;
            public ListNode(int val = 0, ListNode next = null)
            {
                this.val = val;
                this.next = next;
            }
        }
        public ListNode AddTwoNumbers(ListNode l1, ListNode l2) {
            int sum=0;
            int carry=0;
            ListNode previous=null;
            ListNode output=null;
            ListNode temp=null;

            while(l1!=null ||l2!=null)
            {
                sum=carry + (l1!=null?l1.val:0+ l1.val)+ (l2!=null?l2.val:0+ l2.val);
                carry=sum>=10?1:0;
                sum=sum%10;
                temp = new ListNode(sum);
                if(output!=null)
                {
                    previous.next=temp;    
                }
                else
                {
                    output=temp;
                }
                previous=temp;
                if(l1!=null)
                {
                    l1=l1.next;
                }
                 if(l2!=null)
                {
                    l2=l2.next;
                }
            }
            if(carry>0)
                previous.next=new ListNode(1);
            return output;

        }


        public static void DriverCode()

        {
            AddNumbers an = new AddNumbers();
            ListNode l1 = new ListNode(2);
            l1.next= new ListNode(4);
            l1.next.next= new ListNode(3);

            ListNode l2 = new ListNode(5);
            l2.next= new ListNode(6);
            l2.next.next= new ListNode(4);

            an.AddTwoNumbers(l1,l2);


        }

        
    }
}