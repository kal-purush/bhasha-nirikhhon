using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace PatternsPractice.Behavioral
{
    class IteratorPattern
    {
        //public static void Main(string[] args)
        //{
        //    CollectionIteration ci = new CollectionIteration();
        //    foreach (var item in ci)
        //    {
        //        Console.Write(item.ToString() + ' ');
        //    }
        //    Console.ReadKey();
        //}
    }

    abstract class Iteration
    {
        public abstract Iterator GetEnumerator();
    }

    abstract class Iterator
    {
        public abstract object Current { get; }
        public abstract bool MoveNext();
        public abstract void Reset();
    }

    class CollectionIterator : Iterator
    {
        int[] sample = new int[] { 10, 20, 30, 40, 50 };
        public int idx = -1;

        public override Object Current {
            get
            {
                return sample[idx];
            }
        }

        public override bool MoveNext()
        {
            if (idx == sample.Length - 1)
            {
                Reset();
                return false;
            }

            idx++;
            return true;
        }

        public override void Reset()
        {
            idx = -1;
        }
    }

    class CollectionIteration : Iteration
    {
        // Initially it was GetIterator, Changing it to GetEnumerator for it being accepted in foreach
        public override Iterator GetEnumerator()
        {
            return new CollectionIterator();
        }
    }
}