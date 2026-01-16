using System.Collections.Generic;

namespace LeedCodeBusiness.Algos.medium
{
    // 2
    public class LinkedListTwoNumbers
    {
        public LinkedList<int> AddTwoNumbers(LinkedList<int> List1, LinkedList<int> List2)
        {
            var output = new LinkedList<int>();

            //for (int i = 0; i < 3; i++)
            //{
            //    int muh = (int)List1.Last() + (int)List2.First();
            //    if (muh >= 10)
            //    {
            //        muh -= 10;
            //       var ink = (int)output.Last() + 1 ;
            //        output.RemoveLast();
            //        output.AddLast(ink);
            //    }
            //    output.AddFirst(muh);
            //    List1.RemoveLast();
            //    List2.RemoveFirst();
            //}

            return output;
        }

        public LinkedList<int> BuildLinkedList(int[] Ints)
        {
            var Ll = new LinkedList<int>();

            for (int i = 0; i < Ints.Length; i++)
            {
                Ll.AddLast(Ints[i]);
            }

            return Ll;
        }
    }
}