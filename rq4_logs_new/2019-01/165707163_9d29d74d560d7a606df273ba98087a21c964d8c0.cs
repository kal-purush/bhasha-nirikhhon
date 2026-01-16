using System;
using LinkedList.Classes;
namespace MergeList
{
    public class Program
    {
        static void Main(string[] args)
        {
            Console.WriteLine("Hello World!");
        }
        public static LList MergeLists(LList lL1, LList lL2)
        {
            lL1.Current = lL1.Head;
            lL2.Current = lL2.Head;

            while (lL1.Current != null)
            {
                Node temp = lL1.Current.Next; 
                lL1.Current.Next = lL2.Current; 
                lL2.Current = temp; 
                lL1.Current = lL1.Current.Next;
            }
            return lL1;
        }
    }
}
