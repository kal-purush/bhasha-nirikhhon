using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace GenericInClass
{
    internal class ClassInGeneric<E> where E:IComparable
    {
        public E Value1, Value2, Value3;

        public ClassInGeneric(E Value1, E Value2, E Value3)  //Refactor 
        {
            this.Value1= Value1;
            this.Value2= Value2;
            this.Value3= Value3;
        }
        public static E GetMax(E Value1, E Value2, E Value3)
        {
            E max = Value1;
            if (max.CompareTo(Value2) > 0)
                max = Value2;
            if (max.CompareTo(Value3) > 0)
                max = Value3;
            return max;
            
        }
    }
}