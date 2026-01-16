using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace Lazy
{
    public interface ILazy<T>
    {
        // Property
        T Get { get; }  
    }
}