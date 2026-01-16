using System;
using System.Collections;
using System.Collections.Generic;

using ITEA_Collections.Common;

using static ITEA_Collections.Common.Extensions;

namespace ITEA_Collections.CustomCollections
{
    public class IteaCollection : IEnumerable, IBaseCollectionUsing
    {
        private object[] collection;
        private int count = 0;

        public IteaCollection()
        {
            collection = new object[128];
        }

        public IEnumerator GetEnumerator()
        {
            return new IteaEnumerator(collection);
        }

        #region IBaseCollectionUsing
        public void Add(object ts)
        {
            collection[count] = ts;
            count++;
        }

        public void AddMany(object[] ts)
        {
            foreach (object item in ts) Add(item);
        }

        public object GetByID(int index)
        {
            try
            {
                return collection[index] ?? throw new IndexOutOfRangeException();
            }
            catch (IndexOutOfRangeException)
            {
                ToConsole("IteaCollection/GetByID: IndexOutOfRangeException", ConsoleColor.Red);
                return null;
            }
        }

        public void RemoveByID(int index)
        {
            throw new NotImplementedException();
        }

        public object[] GetAll()
        {
            return collection;
        }

        public void Clear()
        {
            collection = new object[128];
        }

        public void ShowAll()
        {
            throw new NotImplementedException();
        }
        #endregion

        public IEnumerable<object> ToEnumerable()
        {
            foreach (var item in collection)
            {
                yield return item;
            }
        }

        public IEnumerable<string> ToEnumerableOnlyStrings()
        {
            foreach (var item in collection)
            {
                if (item is string i)//if(item.GetType().Name == "String") string i = (string)item;                        
                    yield return i;
            }
        }
    }
}