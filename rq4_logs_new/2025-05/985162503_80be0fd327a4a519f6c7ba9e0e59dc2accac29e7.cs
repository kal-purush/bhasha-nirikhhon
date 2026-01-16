using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace AddingElementApp.Extentions
{
    internal static class ListExtentions
    {
        static public List<TClass> PrependValue<TClass>(this List<TClass> list, TClass item)
        {
            List<TClass> result = new List<TClass>();
            result.Add(item);
            result.AddRange(list);
            return result;
        }
    }
}