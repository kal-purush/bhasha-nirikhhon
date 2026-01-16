using System;
using Stack_manual;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace Stack_manual.Extention
{
   public static class StackGenericExtention
    {
            public static int CapacityInt<T>(this StackGeneric<T> stackGeneric)
            {
                int result;
                result = stackGeneric.Lenght-1 - stackGeneric.GetIndex;
                return result;
            }

        public static void CapacityString<T>(this StackGeneric<T> stackGeneric)
        {
            int result;
            result = stackGeneric.Lenght-1 - stackGeneric.GetIndex;
            Console.ForegroundColor = ConsoleColor.Magenta;
            Console.WriteLine($"StackGeneric  has  {result} free item(s).");
            Console.ResetColor();

        }
    }
}