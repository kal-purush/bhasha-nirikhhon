using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace CircuitCalculation
{
    
    public class EventDrivenList<T> : List<T>
    {
        public event EventHandler ItemAdded;
        public event EventHandler ItemRemoved;

        public new void Add(T item)
        {
            base.Add(item);
            if (ItemAdded != null)
            {
                ItemAdded(this, null);
            }
        }

        public new void RemoveAt(int index)
        {
            base.RemoveAt(index);
            if (ItemRemoved != null)
            {
                ItemRemoved(this, null);
            }
        }
    }
}