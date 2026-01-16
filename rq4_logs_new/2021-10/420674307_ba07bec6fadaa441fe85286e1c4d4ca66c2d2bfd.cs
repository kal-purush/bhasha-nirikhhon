using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace Generics_UC_3
{
    class StringMax_UC3
    {
        public string first, second, third;

        public StringMax_UC3(string first , string second ,string third)
        {
            this.first = first;
            this.second = second;
            this.third = third;
        }
        public void FindMax ()
        {                                                                
            if (first.CompareTo(second) >0 && first.CompareTo(third)>0) //we comparing the first value to the second value grearter then zero 
            {                                                           //first value comparing to third value greater then zero .
                Console.WriteLine("The greater string is {0} ",first);
                Console.ReadLine();
            }
            else if (second.CompareTo(first) > 0 && second.CompareTo(third) > 0)
            {
                Console.WriteLine("The greater string is {0} ", first);
                Console.ReadLine();
            }

            else if (third.CompareTo(first) > 0 && third.CompareTo(second)> 0)
            {
                Console.WriteLine("The greater string is {0} ", first);
                Console.ReadLine();
            }
            else
            {
                Console.WriteLine("string first , string second and string third is same ");
                Console.ReadLine();
            }                                                                       //all condition are fails then print else statment.




        }
            

    }
}