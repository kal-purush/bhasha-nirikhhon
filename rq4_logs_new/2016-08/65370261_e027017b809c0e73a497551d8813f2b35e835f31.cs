using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace LinkedList
{
    public static class DeleteMiddleNode<T>
    {
        public static MyList<T> Method_1(MyList<T> list)
        {
            ListNode<T> head = list.Head;
            int cnt = 0;
            while (head != null)
            {
                ++cnt;
                head = head.Next;
            }
            cnt = cnt/2;
            head = list.Head;
            while (cnt >2)
            {
                --cnt;
                head = head.Next;
            }
            head.Next = head.Next.Next;
            return list;
        }
    }
}