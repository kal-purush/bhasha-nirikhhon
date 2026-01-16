using System;
using System.Collections;
using System.Collections.Generic;
using System.Text;

namespace LoliSQLLib.Collections
{
    public class Table<T> : IEnumerable<T>
    {

        private List<T> _items;

        public IEnumerator<T> GetEnumerator()
        {
            throw new NotImplementedException();
        }

        IEnumerator IEnumerable.GetEnumerator() => this.GetEnumerator();
    }
}