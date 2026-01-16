using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace Generic_Maximum
{
    public class Max_Num
    {
        public int MaxNum()
        {

            Console.WriteLine("Enter First Integer number");
            int num1 = Convert.ToInt32(Console.ReadLine());

            Console.WriteLine("Enter Second Integer number");
            int num2 = Convert.ToInt32(Console.ReadLine());

            Console.WriteLine("Enter Third Integer number");
            int num3 = Convert.ToInt32(Console.ReadLine());

            if (num1.CompareTo(num2) > 0 && num1.CompareTo(num3) > 0 ||
               num1.CompareTo(num2) >= 0 && num1.CompareTo(num3) > 0 ||
               num1.CompareTo(num2) > 0 && num1.CompareTo(num3) >= 0)
            {
                return num1;
            }


            if (num2.CompareTo(num1) > 0 && num2.CompareTo(num3) > 0 ||
               num2.CompareTo(num1) >= 0 && num2.CompareTo(num3) > 0 ||
               num2.CompareTo(num1) > 0 && num2.CompareTo(num3) >= 0)
            {
                return num2;
            }

            if (num3.CompareTo(num1) > 0 && num3.CompareTo(num2) > 0 ||
                 num3.CompareTo(num1) >= 0 && num3.CompareTo(num2) > 0 ||
                num3.CompareTo(num1) > 0 && num3.CompareTo(num2) >= 0)
            {
                return num3;
            }
            return num1; 

        }

    }
}