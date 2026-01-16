using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace ProductsStorage
{
    class ContainableItem : Product
    {
        private int row;
        private int column;
        private int size;

        public ContainableItem(string _name, double _price, ProductCategory _category)
            : base(_name, _price, _category)
        {

        }

        public void SetPosition(int _row, int _column)
        {
            if ((_row >= 0 && _row <= 7) && (_column >= 0 && _column <= 5))
            {
                row = _row;
                column = _column;
            }
        }

        public int Size
        {
            get
            {
                if (size <= 0 && size > 5)
                    throw new ArgumentException("Size must be between 1 and 5");
                else
                    return size;
            }
            set
            {
                if (value >= 1 && value <= 5)
                    size = value;
            }
        }
    }
}