using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace SortedMaxValue
{
    class MaxGenericSort<T> where T : IComparable
    {
        public T[] Value;
        //constructor calling
        public MaxGenericSort(T[] Value)
        {
            this.Value= Value;
        }
        //sorting method
        public T[] Sort(T[] Values)
        {
            Array.Sort(Values);
            return Values;
        }
        public T MaxValue(params T[] Values)
        {
            var sorted_values = Sort(Values);
            return sorted_values[^1];
        }
        public T MaxMethod()
        {
            var max=MaxValue(this.Value);
            return max;
        }
        public void PrintValue()
        {
            var max=MaxValue(this.Value);
            Console.WriteLine("The max value is "+max);
        }
    }
}