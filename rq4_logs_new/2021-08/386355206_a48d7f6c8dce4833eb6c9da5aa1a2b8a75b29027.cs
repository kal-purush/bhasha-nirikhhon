using System;
using System.Linq;


namespace ArraysKatas
{
    public class EnumerableMagicOne
    {
        /* Create a method all which takes an array and a predicate (function pointer),
           and returns true if the predicate returns true for every element in the array.
           Otherwise, it should return false. If the array is empty, it should return true,
           since technically nothing failed the test. */
        
        public static bool All(int[] arr, Func<int, bool> fun)
        {
            // for (int i = 0; i < arr.Length; i++)
            // {
            //     if (!fun(arr[i]))
            //     {
            //         return false;
            //     }
            // }
            // return true;
            return arr.All(fun);
        }
    }
}