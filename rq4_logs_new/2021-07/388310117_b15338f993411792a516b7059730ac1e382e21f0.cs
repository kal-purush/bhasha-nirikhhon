using Lab08Collections.interfaces;
using System;
using System.Collections;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace Lab08Collections.classes
{
    //Declaring a new class Backback which implements the interface IBag.
    public class Satchel<T> : IBag<T>
    {
        //Creating an instance of a generic List, which takes in book objects.
        private readonly List<T> stuff = new List<T>();

        public void Pack(T item)
        {
            stuff.Add(item);
        }

        public T Unpack(int index)
        {
            T thing = stuff[index];
            stuff.RemoveAt(index);
            return thing;
        }

        public IEnumerator<T> GetEnumerator()
        {
            foreach (T thing in stuff)
            {
                yield return thing;
            }
        }
        IEnumerator IEnumerable.GetEnumerator()
        {
            return GetEnumerator();
        }

        IEnumerator<T> IEnumerable<T>.GetEnumerator()
        {
            throw new NotImplementedException();
        }
    }
}