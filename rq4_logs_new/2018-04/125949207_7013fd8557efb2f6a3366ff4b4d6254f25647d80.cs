using HashTables;
using System;
using System.Collections;
using System.Collections.Generic;
using System.Text;

namespace HashTablesTesting
{
    class GetHashTestData : IEnumerable<object[]>
    {
        public IEnumerator<object[]> GetEnumerator()
        {
            for (int i = 0; i < 31; i ++)
            {
                yield return new object[] { i };
                yield return new object[] { Math.Pow(2.0, i) };
                yield return new object[] { Math.Pow(3.0, i) };
            }
        }

        IEnumerator IEnumerable.GetEnumerator()
        {
            return GetEnumerator();
        }
    }
}