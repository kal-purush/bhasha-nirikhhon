using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace ConsoleApp2
{
    public class Program
        {
            public class SubstrSearch
            {
                private static string str;
                public void getStr(string _str)
                {
                    str = _str;
                }
                public bool twoElementsDetect(string x)
                {
                    for (int t = 0; t < x.Length - 1; t++)
                    {
                        if (x[t] == x[t + 1]) return true;
                    }
                    return false;
                }
                public void search()
                {
                    for (int i = 0; i < str.Length; i++)
                    {
                        for (int k = 2; k < str.Length - i + 1; k++)
                        {
                            if (twoElementsDetect(str.Substring(i, k))) break;
                            Console.WriteLine(str.Substring(i, k));
                        }
                    }
                }
            }
            public static void Main(string[] args)
            {
            if (args.Length > 0 && args[0].Length > 2)
            {
                for (int i = 0; i < args.Length; i++)
                {
                    SubstrSearch a = new SubstrSearch();
                    a.getStr(args[i]);
                    a.search();
                }
            }
        }
    }
}