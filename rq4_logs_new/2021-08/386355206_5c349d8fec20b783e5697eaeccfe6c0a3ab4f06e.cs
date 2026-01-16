using System;
using System.Linq;
using static System.Convert;

namespace ArraysKatas
{
    public class SumMixedArray
    {
        public static int SumMix(object[] x)
        {
            // var stringParsing = x.Select(ToInt32(x)).ToArray();
            // int sum = x.Sum(stringParsing);
            // return sum;
            
            // 1
            return x.Sum(n => int.Parse(n.ToString()));
            
            // 2
            // return x.Sum(ToInt32);
            
            //3
            // return x.Sum(v => (v as string == null) ? (int)v : Int32.Parse((string)v));
        }
    }
}