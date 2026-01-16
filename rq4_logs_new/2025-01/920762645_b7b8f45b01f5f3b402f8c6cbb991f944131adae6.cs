using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace InstagramProject.Models
{
    public class GenericClass<T> where T : class
    {
        private readonly List<T> _items = new List<T>();

        public void Add(T item)
        {
            _items.Add(item);
        }

        public void Update(T item, Func<T, bool> predicate)
        {
            var itemToUpdate = _items.FirstOrDefault(predicate);
            if (itemToUpdate != null)
            {
                var index = _items.IndexOf(itemToUpdate);
                _items[index] = item;
            }
        }

        public void Remove(Func<T, bool> predicate)
        {
            var itemToRemove = _items.FirstOrDefault(predicate);
            if (itemToRemove != null)
            {
                _items.Remove(itemToRemove);
            }
        }

        public IEnumerable<T> GetAll()
        {
            return _items;
        }
    }
}