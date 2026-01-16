using System.Collections.Generic;

namespace MinMaxIntConcat
{
    public static class StringListExtension
    {
        public delegate int Comparer(string x, string y);

        public static void InsertSorted(this List<string> list, string element, Comparer comparer)
        {
            for (int i = 0; i < list.Count; ++i)
            {
                if (comparer(element, list[i]) > -1)
                {
                    list.Insert(i, element);
                    return;
                }
            }
            list.Add(element);
        }
    }
}