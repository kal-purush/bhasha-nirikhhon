using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace Tushkanchik.Transaction.Categories
{
    public abstract class Categories
    {
        private List<string> _categories { get; set; }

        public Categories()
        {
            _categories = new List<string>();
        }
        public bool Contains(string category)
        {
            return _categories.Contains(category);

        }
        public void Add(string category)
        {
            if (!Contains(category))
            {
                _categories.Add(category);
            }
            else
            {
                throw new Exception("Такая категория существует!");
            }
        }
        public void Remove(string category)
        {
            if (Contains(category))
            {
                _categories.Remove(category);
            }
            else
            {
                throw new Exception("Такой категории нет!");
            }
        }
    }
}